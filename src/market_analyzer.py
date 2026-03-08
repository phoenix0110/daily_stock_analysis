# -*- coding: utf-8 -*-
"""
===================================
大盘复盘分析模块
===================================

职责：
1. 获取大盘指数数据（上证、深证、创业板）
2. 搜索市场新闻形成复盘情报
3. 使用大模型生成每日大盘复盘报告
"""

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any, List

import akshare as ak
import pandas as pd

from src.config import get_config
from data_provider.tushare_fetcher import TushareFetcher
from src.search_service import SearchService
from data_provider.base import DataFetcherManager

logger = logging.getLogger(__name__)


@dataclass
class MarketIndex:
    """大盘指数数据"""
    code: str                    # 指数代码
    name: str                    # 指数名称
    current: float = 0.0         # 当前点位
    change: float = 0.0          # 涨跌点数
    change_pct: float = 0.0      # 涨跌幅(%)
    open: float = 0.0            # 开盘点位
    high: float = 0.0            # 最高点位
    low: float = 0.0             # 最低点位
    prev_close: float = 0.0      # 昨收点位
    volume: float = 0.0          # 成交量（手）
    amount: float = 0.0          # 成交额（元）
    amplitude: float = 0.0       # 振幅(%)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'code': self.code,
            'name': self.name,
            'current': self.current,
            'change': self.change,
            'change_pct': self.change_pct,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'volume': self.volume,
            'amount': self.amount,
            'amplitude': self.amplitude,
        }


@dataclass
class MarketOverview:
    """市场概览数据"""
    date: str                           # 日期
    indices: List[MarketIndex] = field(default_factory=list)  # 主要指数
    up_count: int = 0                   # 上涨家数
    down_count: int = 0                 # 下跌家数
    flat_count: int = 0                 # 平盘家数
    limit_up_count: int = 0             # 涨停家数
    limit_down_count: int = 0           # 跌停家数
    total_amount: float = 0.0           # 两市成交额（亿元）
    # north_flow: float = 0.0           # 北向资金净流入（亿元）- 已废弃，接口不可用
    
    # 板块涨幅榜
    top_sectors: List[Dict] = field(default_factory=list)     # 涨幅前5板块
    bottom_sectors: List[Dict] = field(default_factory=list)  # 跌幅前5板块


class MarketAnalyzer:
    """
    大盘复盘分析器
    
    功能：
    1. 获取大盘指数实时行情
    2. 获取市场涨跌统计
    3. 获取板块涨跌榜
    4. 搜索市场新闻
    5. 生成大盘复盘报告
    """
    
    # 主要指数代码
    MAIN_INDICES = {
        'sh000001': '上证指数',
        'sz399001': '深证成指',  
        'sz399006': '创业板指',
        'sh000688': '科创50',
        'sh000016': '上证50',
        'sh000300': '沪深300',
    }
    
    def __init__(
        self, 
        search_service: Optional[SearchService] = None, 
        analyzer=None,
        tushare_fetcher: Optional[TushareFetcher] = None
    ):
        """
        初始化大盘分析器

        Args:
            search_service: 搜索服务实例
            analyzer: AI分析器实例（用于调用LLM）
            tushare_fetcher: Tushare 数据源（可选，用于备选获取大盘数据）
        """
        self.config = get_config()
        self.search_service = search_service
        self.analyzer = analyzer
        self.data_manager = DataFetcherManager()
        # Tushare 数据源（根据 priority 决定是否优先使用）
        self.tushare_fetcher = tushare_fetcher or TushareFetcher()
        
        # 判断是否优先使用 Tushare（当 Tushare 可用且优先级为 0 时）
        self._prefer_tushare = (
            self.tushare_fetcher.is_available and 
            self.tushare_fetcher.priority == 0
        )
        if self._prefer_tushare:
            logger.info("[大盘] 检测到 Tushare 配置，将优先使用 Tushare 数据源")
        
    def get_market_overview(self) -> MarketOverview:
        """
        获取市场概览数据
        
        Returns:
            MarketOverview: 市场概览数据对象
        """
        today = datetime.now().strftime('%Y-%m-%d')
        overview = MarketOverview(date=today)
        
        # 1. 获取主要指数行情
        overview.indices = self._get_main_indices()
        
        # 2. 获取涨跌统计
        self._get_market_statistics(overview)
        
        # 3. 获取板块涨跌榜
        self._get_sector_rankings(overview)
        
        # 4. 获取北向资金（可选）
        # self._get_north_flow(overview)
        
        return overview

    
    def _get_main_indices(self) -> List[MarketIndex]:
        """
        获取主要指数实时行情
        
        数据源优先级：
        1. 如果 Tushare 配置了 Token 且可用（priority=0），优先使用 Tushare
        2. 否则使用 AkShare，失败时回退到 Tushare
        """
        indices = []
        
        logger.info("[大盘] 获取主要指数实时行情...")
        
        # 如果优先使用 Tushare
        if self._prefer_tushare:
            logger.info("[大盘] 优先使用 Tushare 获取指数行情...")
            indices = self._get_indices_from_tushare()
            
            # Tushare 失败，回退到 AkShare
            if not indices:
                logger.warning("[大盘] Tushare 获取指数失败，切换到 AkShare...")
                indices = self._get_indices_from_akshare()
        else:
            # 默认使用 AkShare
            indices = self._get_indices_from_akshare()
            
            # AkShare 失败，回退到 Tushare
            if not indices and self.tushare_fetcher and self.tushare_fetcher.is_available:
                logger.warning("[大盘] AkShare 获取指数失败，切换到 Tushare 备选...")
                indices = self._get_indices_from_tushare()
        
        return indices
    
    def _get_indices_from_akshare(self) -> List[MarketIndex]:
        """
        从 AkShare 获取指数行情
        
        Returns:
            指数列表
        """
        indices = []

        try:
            # 使用 akshare 获取指数行情（新浪财经接口，包含深市指数）
            df = self._call_akshare_with_retry(ak.stock_zh_index_spot_sina, "指数行情", attempts=2)
            
            if df is not None and not df.empty:
                for code, name in self.MAIN_INDICES.items():
                    # 查找对应指数
                    row = df[df['代码'] == code]
                    if row.empty:
                        # 尝试带前缀查找
                        row = df[df['代码'].str.contains(code)]
                    
                    if not row.empty:
                        row = row.iloc[0]
                        index = MarketIndex(
                            code=code,
                            name=name,
                            current=float(row.get('最新价', 0) or 0),
                            change=float(row.get('涨跌额', 0) or 0),
                            change_pct=float(row.get('涨跌幅', 0) or 0),
                            open=float(row.get('今开', 0) or 0),
                            high=float(row.get('最高', 0) or 0),
                            low=float(row.get('最低', 0) or 0),
                            prev_close=float(row.get('昨收', 0) or 0),
                            volume=float(row.get('成交量', 0) or 0),
                            amount=float(row.get('成交额', 0) or 0),
                        )
                        # 计算振幅
                        if index.prev_close > 0:
                            index.amplitude = (index.high - index.low) / index.prev_close * 100
                        indices.append(index)
                        
                logger.info(f"[大盘] AkShare 获取到 {len(indices)} 个指数行情")
                
        except Exception as e:
            logger.error(f"[大盘] AkShare 获取指数行情失败: {e}")
        
        return indices
    
    def _get_indices_from_tushare(self) -> List[MarketIndex]:
        """
        从 Tushare 获取指数行情（备选方案）
        
        Returns:
            指数列表
        """
        indices = []
        
        try:
            tushare_data = self.tushare_fetcher.get_index_daily()
            
            for data in tushare_data:
                index = MarketIndex(
                    code=data['code'],
                    name=data['name'],
                    current=data.get('current', 0),
                    change=data.get('change', 0),
                    change_pct=data.get('change_pct', 0),
                    open=data.get('open', 0),
                    high=data.get('high', 0),
                    low=data.get('low', 0),
                    prev_close=data.get('pre_close', 0),
                    volume=data.get('volume', 0),
                    amount=data.get('amount', 0),
                )
                # 计算振幅
                if index.prev_close > 0:
                    index.amplitude = (index.high - index.low) / index.prev_close * 100
                indices.append(index)
            
            if indices:
                logger.info(f"[大盘] Tushare 获取到 {len(indices)} 个指数行情")
                
        except Exception as e:
            logger.error(f"[大盘] Tushare 获取指数行情失败: {e}")
        
        return indices
    
    def _get_market_statistics(self, overview: MarketOverview):
        """
        获取市场涨跌统计
        
        数据源优先级：
        1. 如果 Tushare 配置了 Token 且可用（priority=0），优先使用 Tushare
        2. 否则使用 AkShare，失败时回退到 Tushare
        """
        logger.info("[大盘] 获取市场涨跌统计...")
        
        success = False
        
        # 如果优先使用 Tushare
        if self._prefer_tushare:
            logger.info("[大盘] 优先使用 Tushare 获取涨跌统计...")
            success = self._get_market_statistics_from_tushare(overview)
            
            # Tushare 失败，回退到 AkShare
            if not success:
                logger.warning("[大盘] Tushare 获取涨跌统计失败，切换到 AkShare...")
                success = self._get_market_statistics_from_akshare(overview)
        else:
            # 默认使用 AkShare
            success = self._get_market_statistics_from_akshare(overview)
            
            # AkShare 失败，回退到 Tushare
            if not success and self.tushare_fetcher and self.tushare_fetcher.is_available:
                logger.warning("[大盘] AkShare 获取涨跌统计失败，切换到 Tushare 备选...")
                self._get_market_statistics_from_tushare(overview)
    
    def _get_market_statistics_from_akshare(self, overview: MarketOverview) -> bool:
        """
        从 AkShare 获取市场涨跌统计
        
        Args:
            overview: 市场概览对象，会被修改
            
        Returns:
            是否获取成功
        """
        try:
            # 获取全部A股实时行情（AkShare）
            df = self._call_akshare_with_retry(ak.stock_zh_a_spot_em, "A股实时行情", attempts=2)
            
            if df is not None and not df.empty:
                # 涨跌统计
                change_col = '涨跌幅'
                if change_col in df.columns:
                    df[change_col] = pd.to_numeric(df[change_col], errors='coerce')
                    overview.up_count = len(df[df[change_col] > 0])
                    overview.down_count = len(df[df[change_col] < 0])
                    overview.flat_count = len(df[df[change_col] == 0])
                    
                    # 涨停跌停统计（涨跌幅 >= 9.9% 或 <= -9.9%）
                    overview.limit_up_count = len(df[df[change_col] >= 9.9])
                    overview.limit_down_count = len(df[df[change_col] <= -9.9])
                
                # 两市成交额
                amount_col = '成交额'
                if amount_col in df.columns:
                    df[amount_col] = pd.to_numeric(df[amount_col], errors='coerce')
                    overview.total_amount = df[amount_col].sum() / 1e8  # 转为亿元
                
                logger.info(f"[大盘] AkShare 涨跌统计: 涨:{overview.up_count} 跌:{overview.down_count} 平:{overview.flat_count} "
                          f"涨停:{overview.limit_up_count} 跌停:{overview.limit_down_count} "
                          f"成交额:{overview.total_amount:.0f}亿")
                return True
                
        except Exception as e:
            logger.error(f"[大盘] AkShare 获取涨跌统计失败: {e}")
        
        return False
    
    def _get_market_statistics_from_tushare(self, overview: MarketOverview) -> bool:
        """
        从 Tushare 获取市场涨跌统计
        
        Args:
            overview: 市场概览对象，会被修改
            
        Returns:
            是否获取成功
        """
        try:
            stats = self.tushare_fetcher.get_market_daily_stats()
            
            if stats:
                overview.up_count = stats.get('up_count', 0)
                overview.down_count = stats.get('down_count', 0)
                overview.flat_count = stats.get('flat_count', 0)
                overview.limit_up_count = stats.get('limit_up_count', 0)
                overview.limit_down_count = stats.get('limit_down_count', 0)
                # 成交额（如果有的话）
                if stats.get('total_amount'):
                    overview.total_amount = stats.get('total_amount', 0)
                
                logger.info(f"[大盘] Tushare 涨跌统计: 涨:{overview.up_count} 跌:{overview.down_count} "
                          f"平:{overview.flat_count} 涨停:{overview.limit_up_count} 跌停:{overview.limit_down_count}")
                return True
        except Exception as e:
            logger.error(f"[大盘] Tushare 获取涨跌统计失败: {e}")
        
        return False
    
    def _get_sector_rankings(self, overview: MarketOverview):
        """
        获取板块涨跌榜
        
        数据源优先级：
        1. 如果 Tushare 配置了 Token 且可用（priority=0），优先使用 Tushare
        2. 否则使用 AkShare，失败时回退到 Tushare
        """
        logger.info("[大盘] 获取板块涨跌榜...")
        
        success = False
        
        # 如果优先使用 Tushare
        if self._prefer_tushare:
            logger.info("[大盘] 优先使用 Tushare 获取板块涨跌榜...")
            success = self._get_sector_rankings_from_tushare(overview)
            
            # Tushare 失败，回退到 AkShare
            if not success:
                logger.warning("[大盘] Tushare 获取板块失败，切换到 AkShare...")
                success = self._get_sector_rankings_from_akshare(overview)
        else:
            # 默认使用 AkShare
            success = self._get_sector_rankings_from_akshare(overview)
            
            # AkShare 失败，回退到 Tushare
            if not success and self.tushare_fetcher and self.tushare_fetcher.is_available:
                logger.warning("[大盘] AkShare 获取板块失败，切换到 Tushare 备选...")
                success = self._get_sector_rankings_from_tushare(overview)
        
        if not success:
            logger.warning("[大盘] 板块行情获取失败，报告中将不显示板块信息")
    
    def _get_sector_rankings_from_akshare(self, overview: MarketOverview) -> bool:
        """
        从 AkShare 获取板块涨跌榜
        
        Args:
            overview: 市场概览对象，会被修改
            
        Returns:
            是否获取成功
        """
        try:
            # 获取行业板块行情
            df = self._call_akshare_with_retry(ak.stock_board_industry_name_em, "行业板块行情", attempts=2)
            
            if df is not None and not df.empty:
                change_col = '涨跌幅'
                if change_col in df.columns:
                    df[change_col] = pd.to_numeric(df[change_col], errors='coerce')
                    df = df.dropna(subset=[change_col])
                    
                    # 涨幅前5
                    top = df.nlargest(5, change_col)
                    overview.top_sectors = [
                        {'name': row['板块名称'], 'change_pct': row[change_col]}
                        for _, row in top.iterrows()
                    ]
                    
                    # 跌幅前5
                    bottom = df.nsmallest(5, change_col)
                    overview.bottom_sectors = [
                        {'name': row['板块名称'], 'change_pct': row[change_col]}
                        for _, row in bottom.iterrows()
                    ]
                    
                    logger.info(f"[大盘] AkShare 领涨板块: {[s['name'] for s in overview.top_sectors]}")
                    logger.info(f"[大盘] AkShare 领跌板块: {[s['name'] for s in overview.bottom_sectors]}")
                    return True
                    
        except Exception as e:
            logger.warning(f"[大盘] AkShare 获取板块涨跌榜失败: {e}")
        
        return False
    
    def _get_sector_rankings_from_tushare(self, overview: MarketOverview) -> bool:
        """
        从 Tushare 获取板块涨跌榜（使用同花顺板块指数）
        
        需要 6000 积分以上
        
        Args:
            overview: 市场概览对象，会被修改
            
        Returns:
            是否获取成功
        """
        try:
            if not self.tushare_fetcher or not self.tushare_fetcher.is_available:
                logger.warning("[大盘] Tushare 不可用，无法获取板块数据")
                return False
            
            result = self.tushare_fetcher.get_sector_rankings()
            
            if result and result.get('top_sectors') and result.get('bottom_sectors'):
                overview.top_sectors = result['top_sectors']
                overview.bottom_sectors = result['bottom_sectors']
                
                logger.info(f"[大盘] Tushare 领涨板块: {[s['name'] for s in overview.top_sectors]}")
                logger.info(f"[大盘] Tushare 领跌板块: {[s['name'] for s in overview.bottom_sectors]}")
                return True
            
        except Exception as e:
            logger.warning(f"[大盘] Tushare 获取板块涨跌榜失败: {e}")
        
        return False
    
    # def _get_north_flow(self, overview: MarketOverview):
    #     """获取北向资金流入"""
    #     try:
    #         logger.info("[大盘] 获取北向资金...")
            
    #         # 获取北向资金数据
    #         df = ak.stock_hsgt_north_net_flow_in_em(symbol="北上")
            
    #         if df is not None and not df.empty:
    #             # 取最新一条数据
    #             latest = df.iloc[-1]
    #             if '当日净流入' in df.columns:
    #                 overview.north_flow = float(latest['当日净流入']) / 1e8  # 转为亿元
    #             elif '净流入' in df.columns:
    #                 overview.north_flow = float(latest['净流入']) / 1e8
                    
    #             logger.info(f"[大盘] 北向资金净流入: {overview.north_flow:.2f}亿")
                
    #     except Exception as e:
    #         logger.warning(f"[大盘] 获取北向资金失败: {e}")
    
    def search_market_news(self) -> List[Dict]:
        """
        搜索市场新闻
        
        Returns:
            新闻列表
        """
        if not self.search_service:
            logger.warning("[大盘] 搜索服务未配置，跳过新闻搜索")
            return []
        
        all_news = []
        today = datetime.now()
        date_str = today.strftime('%Y年%m月%d日')

        # 多维度搜索
        search_queries = [
            "A股 大盘 复盘",
            "股市 行情 分析",
            "A股 市场 热点 板块",
        ]
        
        try:
            logger.info("[大盘] 开始搜索市场新闻...")
            
            for query in search_queries:
                # 使用 search_stock_news 方法，传入"大盘"作为股票名
                response = self.search_service.search_stock_news(
                    stock_code="market",
                    stock_name="大盘",
                    max_results=3,
                    focus_keywords=query.split()
                )
                if response and response.results:
                    all_news.extend(response.results)
                    logger.info(f"[大盘] 搜索 '{query}' 获取 {len(response.results)} 条结果")
            
            logger.info(f"[大盘] 共获取 {len(all_news)} 条市场新闻")
            
        except Exception as e:
            logger.error(f"[大盘] 搜索市场新闻失败: {e}")
        
        return all_news
    
    def generate_market_review(self, overview: MarketOverview, news: List) -> str:
        """
        使用大模型生成大盘复盘报告
        
        Args:
            overview: 市场概览数据
            news: 市场新闻列表 (SearchResult 对象列表)
            
        Returns:
            大盘复盘报告文本
        """
        if not self.analyzer or not self.analyzer.is_available():
            logger.warning("[大盘] AI分析器未配置或不可用，使用模板生成报告")
            return self._generate_template_review(overview, news)
        
        # 构建 Prompt
        prompt = self._build_review_prompt(overview, news)
        
        try:
            logger.info("[大盘] 调用大模型生成复盘报告...")
            
            generation_config = {
                'temperature': 0.7,
                'max_output_tokens': 2048,
            }
            
            # 根据 analyzer 使用的 API 类型调用
            if self.analyzer._use_openai:
                # 使用 OpenAI 兼容 API
                review = self.analyzer._call_openai_api(prompt, generation_config)
            else:
                # 使用 Gemini API
                response = self.analyzer._model.generate_content(
                    prompt,
                    generation_config=generation_config,
                )
                review = response.text.strip() if response and response.text else None
            
            if review:
                logger.info(f"[大盘] 复盘报告生成成功，长度: {len(review)} 字符")
                return review
            else:
                logger.warning("[大盘] 大模型返回为空")
                return self._generate_template_review(overview, news)
                
        except Exception as e:
            logger.error(f"[大盘] 大模型生成复盘报告失败: {e}")
            return self._generate_template_review(overview, news)
    
    def _build_review_prompt(self, overview: MarketOverview, news: List) -> str:
        """构建复盘报告 Prompt"""
        # 指数行情信息（简洁格式，不用emoji）
        indices_text = ""
        for idx in overview.indices:
            direction = "↑" if idx.change_pct > 0 else "↓" if idx.change_pct < 0 else "-"
            indices_text += f"- {idx.name}: {idx.current:.2f} ({direction}{abs(idx.change_pct):.2f}%)\n"
        
        # 板块信息
        top_sectors_text = ", ".join([f"{s['name']}({s['change_pct']:+.2f}%)" for s in overview.top_sectors[:3]])
        bottom_sectors_text = ", ".join([f"{s['name']}({s['change_pct']:+.2f}%)" for s in overview.bottom_sectors[:3]])
        
        # 新闻信息 - 支持 SearchResult 对象或字典
        news_text = ""
        for i, n in enumerate(news[:6], 1):
            # 兼容 SearchResult 对象和字典
            if hasattr(n, 'title'):
                title = n.title[:50] if n.title else ''
                snippet = n.snippet[:100] if n.snippet else ''
            else:
                title = n.get('title', '')[:50]
                snippet = n.get('snippet', '')[:100]
            news_text += f"{i}. {title}\n   {snippet}\n"
        
        prompt = f"""你是一位专业的A/H/美股市场分析师，请根据以下数据生成一份简洁的大盘复盘报告。

【重要】输出要求：
- 必须输出纯 Markdown 文本格式
- 禁止输出 JSON 格式
- 禁止输出代码块
- emoji 仅在标题处少量使用（每个标题最多1个）

---

# 今日市场数据

## 日期
{overview.date}

## 主要指数
{indices_text}

## 市场概况
- 上涨: {overview.up_count} 家 | 下跌: {overview.down_count} 家 | 平盘: {overview.flat_count} 家
- 涨停: {overview.limit_up_count} 家 | 跌停: {overview.limit_down_count} 家
- 两市成交额: {overview.total_amount:.0f} 亿元

## 板块表现
领涨: {top_sectors_text}
领跌: {bottom_sectors_text}

## 市场新闻
{news_text if news_text else "暂无相关新闻"}

---

# 输出格式模板（请严格按此格式输出）

## 📊 {overview.date} 大盘复盘

### 一、市场总结
（2-3句话概括今日市场整体表现，包括指数涨跌、成交量变化）

### 二、指数点评
（分析上证、深证、创业板等各指数走势特点）

### 三、资金动向
（解读成交额流向的含义）

### 四、热点解读
（分析领涨领跌板块背后的逻辑和驱动因素）

### 五、后市展望
（结合当前走势和新闻，给出明日市场预判）

### 六、风险提示
（需要关注的风险点）

---

请直接输出复盘报告内容，不要输出其他说明文字。
"""
        return prompt
    
    def _generate_template_review(self, overview: MarketOverview, news: List) -> str:
        """使用模板生成复盘报告（无大模型时的备选方案）"""
        
        # 判断市场走势
        sh_index = next((idx for idx in overview.indices if idx.code == '000001'), None)
        if sh_index:
            if sh_index.change_pct > 1:
                market_mood = "强势上涨"
            elif sh_index.change_pct > 0:
                market_mood = "小幅上涨"
            elif sh_index.change_pct > -1:
                market_mood = "小幅下跌"
            else:
                market_mood = "明显下跌"
        else:
            market_mood = "震荡整理"
        
        # 指数行情（简洁格式）
        indices_text = ""
        for idx in overview.indices[:4]:
            direction = "↑" if idx.change_pct > 0 else "↓" if idx.change_pct < 0 else "-"
            indices_text += f"- **{idx.name}**: {idx.current:.2f} ({direction}{abs(idx.change_pct):.2f}%)\n"
        
        # 板块信息
        top_text = "、".join([s['name'] for s in overview.top_sectors[:3]])
        bottom_text = "、".join([s['name'] for s in overview.bottom_sectors[:3]])
        
        report = f"""## 📊 {overview.date} 大盘复盘

### 一、市场总结
今日A股市场整体呈现**{market_mood}**态势。

### 二、主要指数
{indices_text}

### 三、涨跌统计
| 指标 | 数值 |
|------|------|
| 上涨家数 | {overview.up_count} |
| 下跌家数 | {overview.down_count} |
| 涨停 | {overview.limit_up_count} |
| 跌停 | {overview.limit_down_count} |
| 两市成交额 | {overview.total_amount:.0f}亿 |

### 四、板块表现
- **领涨**: {top_text}
- **领跌**: {bottom_text}

### 五、风险提示
市场有风险，投资需谨慎。以上数据仅供参考，不构成投资建议。

---
*复盘时间: {datetime.now().strftime('%H:%M')}*
"""
        return report
    
    def run_daily_review(self) -> str:
        """
        执行每日大盘复盘流程
        
        Returns:
            复盘报告文本
        """
        logger.info("========== 开始大盘复盘分析 ==========")
        
        # 1. 获取市场概览
        overview = self.get_market_overview()
        
        # 2. 搜索市场新闻
        news = self.search_market_news()
        
        # 3. 生成复盘报告
        report = self.generate_market_review(overview, news)
        
        logger.info("========== 大盘复盘分析完成 ==========")
        
        return report


# 测试入口
if __name__ == "__main__":
    import sys
    sys.path.insert(0, '.')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s',
    )
    
    analyzer = MarketAnalyzer()
    
    # 测试获取市场概览
    overview = analyzer.get_market_overview()
    print(f"\n=== 市场概览 ===")
    print(f"日期: {overview.date}")
    print(f"指数数量: {len(overview.indices)}")
    for idx in overview.indices:
        print(f"  {idx.name}: {idx.current:.2f} ({idx.change_pct:+.2f}%)")
    print(f"上涨: {overview.up_count} | 下跌: {overview.down_count}")
    print(f"成交额: {overview.total_amount:.0f}亿")
    
    # 测试生成模板报告
    report = analyzer._generate_template_review(overview, [])
    print(f"\n=== 复盘报告 ===")
    print(report)
