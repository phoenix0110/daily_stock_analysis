# -*- coding: utf-8 -*-
"""
===================================
TushareFetcher - 备用数据源 1 (Priority 2)
===================================

数据来源：Tushare Pro API（挖地兔）
特点：需要 Token、有请求配额限制
优点：数据质量高、接口稳定

流控策略：
1. 实现"每分钟调用计数器"
2. 超过免费配额（80次/分）时，强制休眠到下一分钟
3. 使用 tenacity 实现指数退避重试

扩展功能（用于替代 AkShare 失败的场景）：
- get_all_stock_names(): 获取所有股票名称映射
- get_index_daily(): 获取大盘指数日线行情
- get_market_daily_stats(): 获取市场涨跌统计
"""

import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from .base import BaseFetcher, DataFetchError, RateLimitError, STANDARD_COLUMNS
from src.config import get_config

logger = logging.getLogger(__name__)


class TushareFetcher(BaseFetcher):
    """
    Tushare Pro 数据源实现
    
    优先级：2
    数据来源：Tushare Pro API
    
    特点：
    - 数据质量高、接口稳定
    - 付费用户配额充足（1万积分用户约 500次/分钟）
    - 失败后指数退避重试
    """
    
    name = "TushareFetcher"
    priority = 2  # 默认优先级，会在 __init__ 中根据配置动态调整

    def __init__(self, rate_limit_per_minute: int = 80):
        """
        初始化 TushareFetcher

        Args:
            rate_limit_per_minute: 每分钟最大请求数（默认80，Tushare免费配额）
        """
        self.rate_limit_per_minute = rate_limit_per_minute
        self._call_count = 0  # 当前分钟内的调用次数
        self._minute_start: Optional[float] = None  # 当前计数周期开始时间
        self._api: Optional[object] = None  # Tushare API 实例

        # 尝试初始化 API
        self._init_api()

        # 根据 API 初始化结果动态调整优先级
        self.priority = self._determine_priority()
    
    def _init_api(self) -> None:
        """
        初始化 Tushare API
        
        支持两种模式：
        1. 标准模式：仅配置 TUSHARE_TOKEN
        2. 自定义服务端点模式：配置 TUSHARE_TOKEN + TUSHARE_HTTP_URL
        
        如果 Token 未配置，此数据源将不可用
        """
        config = get_config()
        
        if not config.tushare_token:
            logger.warning("Tushare Token 未配置，此数据源不可用")
            return
        
        try:
            import tushare as ts
            
            token = config.tushare_token
            
            # Tushare Pro 服务端点
            TUSHARE_HTTP_URL = 'http://106.54.191.157:5000'
            
            # 调试日志：打印 token 前10位和后4位（隐藏中间部分）
            if token:
                token_preview = f"{token[:10]}...{token[-4:]}" if len(token) > 14 else token[:4] + "..."
                logger.info(f"[Tushare] Token: {token_preview}")
            
            # 获取 API 实例（传入 token）
            self._api = ts.pro_api(token)
            
            # 强制设置 token 和 http_url
            self._api._DataApi__token = token
            self._api._DataApi__http_url = TUSHARE_HTTP_URL
            
            logger.info(f"Tushare API 初始化成功（端点: {TUSHARE_HTTP_URL}）")
            
        except Exception as e:
            logger.error(f"Tushare API 初始化失败: {e}")
            self._api = None

    def _determine_priority(self) -> int:
        """
        根据 Token 配置和 API 初始化状态确定优先级

        策略：
        - Token 配置且 API 初始化成功：优先级 0（最高）
        - 其他情况：优先级 2（默认）

        Returns:
            优先级数字（0=最高，数字越大优先级越低）
        """
        config = get_config()

        if config.tushare_token and self._api is not None:
            # Token 配置且 API 初始化成功，提升为最高优先级
            logger.info("✅ 检测到 TUSHARE_TOKEN 且 API 初始化成功，Tushare 数据源优先级提升为最高 (Priority 0)")
            return 0

        # Token 未配置或 API 初始化失败，保持默认优先级
        return 2

    def _check_rate_limit(self) -> None:
        """
        检查并执行速率限制
        
        流控策略：
        1. 检查是否进入新的一分钟
        2. 如果是，重置计数器
        3. 如果当前分钟调用次数超过限制，强制休眠
        """
        current_time = time.time()
        
        # 检查是否需要重置计数器（新的一分钟）
        if self._minute_start is None:
            self._minute_start = current_time
            self._call_count = 0
        elif current_time - self._minute_start >= 60:
            # 已经过了一分钟，重置计数器
            self._minute_start = current_time
            self._call_count = 0
            logger.debug("速率限制计数器已重置")
        
        # 检查是否超过配额
        if self._call_count >= self.rate_limit_per_minute:
            # 计算需要等待的时间（到下一分钟）
            elapsed = current_time - self._minute_start
            sleep_time = max(0, 60 - elapsed) + 1  # +1 秒缓冲
            
            logger.warning(
                f"Tushare 达到速率限制 ({self._call_count}/{self.rate_limit_per_minute} 次/分钟)，"
                f"等待 {sleep_time:.1f} 秒..."
            )
            
            time.sleep(sleep_time)
            
            # 重置计数器
            self._minute_start = time.time()
            self._call_count = 0
        
        # 增加调用计数
        self._call_count += 1
        logger.debug(f"Tushare 当前分钟调用次数: {self._call_count}/{self.rate_limit_per_minute}")
    
    def _convert_stock_code(self, stock_code: str) -> str:
        """
        转换股票代码为 Tushare 格式
        
        Tushare 要求的格式：
        - 沪市：600519.SH
        - 深市：000001.SZ
        
        Args:
            stock_code: 原始代码，如 '600519', '000001'
            
        Returns:
            Tushare 格式代码，如 '600519.SH', '000001.SZ'
        """
        code = stock_code.strip()
        
        # 已经包含后缀的情况
        if '.' in code:
            return code.upper()
        
        # 根据代码前缀判断市场
        # 沪市：600xxx, 601xxx, 603xxx, 688xxx (科创板)
        # 深市：000xxx, 002xxx, 300xxx (创业板)
        if code.startswith(('600', '601', '603', '688')):
            return f"{code}.SH"
        elif code.startswith(('000', '002', '300')):
            return f"{code}.SZ"
        else:
            # 默认尝试深市
            logger.warning(f"无法确定股票 {code} 的市场，默认使用深市")
            return f"{code}.SZ"
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((ConnectionError, TimeoutError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _fetch_raw_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        从 Tushare 获取原始数据
        
        使用 daily() 接口获取日线数据
        
        流程：
        1. 检查 API 是否可用
        2. 执行速率限制检查
        3. 转换股票代码格式
        4. 调用 API 获取数据
        """
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化，请检查 Token 配置")
        
        # 转换代码格式
        ts_code = self._convert_stock_code(stock_code)
        
        # 转换日期格式（Tushare 要求 YYYYMMDD）
        ts_start = start_date.replace('-', '')
        ts_end = end_date.replace('-', '')
        
        logger.debug(f"调用 Tushare daily({ts_code}, {ts_start}, {ts_end})")
        
        try:
            # 调用 daily 接口获取日线数据
            df = self._api.daily(
                ts_code=ts_code,
                start_date=ts_start,
                end_date=ts_end,
            )
            
            return df
            
        except Exception as e:
            error_msg = str(e).lower()
            
            # 检测配额超限
            if any(keyword in error_msg for keyword in ['quota', '配额', 'limit', '权限']):
                logger.warning(f"Tushare 配额可能超限: {e}")
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            
            raise DataFetchError(f"Tushare 获取数据失败: {e}") from e
    
    def _normalize_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """
        标准化 Tushare 数据
        
        Tushare daily 返回的列名：
        ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount
        
        需要映射到标准列名：
        date, open, high, low, close, volume, amount, pct_chg
        """
        df = df.copy()
        
        # 列名映射
        column_mapping = {
            'trade_date': 'date',
            'vol': 'volume',
            # open, high, low, close, amount, pct_chg 列名相同
        }
        
        df = df.rename(columns=column_mapping)
        
        # 转换日期格式（YYYYMMDD -> YYYY-MM-DD）
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
        
        # 成交量单位转换（Tushare 的 vol 单位是手，需要转换为股）
        if 'volume' in df.columns:
            df['volume'] = df['volume'] * 100
        
        # 成交额单位转换（Tushare 的 amount 单位是千元，转换为元）
        if 'amount' in df.columns:
            df['amount'] = df['amount'] * 1000
        
        # 添加股票代码列
        df['code'] = stock_code
        
        # 只保留需要的列
        keep_cols = ['code'] + STANDARD_COLUMNS
        existing_cols = [col for col in keep_cols if col in df.columns]
        df = df[existing_cols]
        
        return df

    # ==================== 扩展功能：股票名称（带缓存） ====================
    
    def get_stock_name(self, stock_code: str) -> Optional[str]:
        """
        获取股票名称
        
        使用 Tushare 的 stock_basic 接口获取股票基本信息
        
        Args:
            stock_code: 股票代码
            
        Returns:
            股票名称，失败返回 None
        """
        if self._api is None:
            logger.warning("Tushare API 未初始化，无法获取股票名称")
            return None
        
        # 检查缓存
        if hasattr(self, '_stock_name_cache') and stock_code in self._stock_name_cache:
            return self._stock_name_cache[stock_code]
        
        # 初始化缓存
        if not hasattr(self, '_stock_name_cache'):
            self._stock_name_cache = {}
        
        try:
            # 速率限制检查
            self._check_rate_limit()
            
            # 转换代码格式
            ts_code = self._convert_stock_code(stock_code)
            
            # 调用 stock_basic 接口
            df = self._api.stock_basic(
                ts_code=ts_code,
                fields='ts_code,name'
            )
            
            if df is not None and not df.empty:
                name = df.iloc[0]['name']
                self._stock_name_cache[stock_code] = name
                logger.debug(f"Tushare 获取股票名称成功: {stock_code} -> {name}")
                return name
            
        except Exception as e:
            logger.warning(f"Tushare 获取股票名称失败 {stock_code}: {e}")
        
        return None
    
    def get_stock_list(self) -> Optional[pd.DataFrame]:
        """
        获取股票列表
        
        使用 Tushare 的 stock_basic 接口获取全部股票列表
        
        Returns:
            包含 code, name 列的 DataFrame，失败返回 None
        """
        if self._api is None:
            logger.warning("Tushare API 未初始化，无法获取股票列表")
            return None
        
        try:
            # 速率限制检查
            self._check_rate_limit()
            
            # 调用 stock_basic 接口获取所有股票
            df = self._api.stock_basic(
                exchange='',
                list_status='L',
                fields='ts_code,name,industry,area,market'
            )
            
            if df is not None and not df.empty:
                # 转换 ts_code 为标准代码格式
                df['code'] = df['ts_code'].apply(lambda x: x.split('.')[0])
                
                # 更新缓存
                if not hasattr(self, '_stock_name_cache'):
                    self._stock_name_cache = {}
                for _, row in df.iterrows():
                    self._stock_name_cache[row['code']] = row['name']
                
                logger.info(f"Tushare 获取股票列表成功: {len(df)} 条")
                return df[['code', 'name', 'industry', 'area', 'market']]
            
        except Exception as e:
            logger.warning(f"Tushare 获取股票列表失败: {e}")
        
        return None
    
    def get_realtime_quote(self, stock_code: str) -> Optional[dict]:
        """
        获取实时行情（Tushare Pro 需要较高积分才能使用实时接口）
        
        注意：Tushare 实时行情接口需要较高积分（>=2000），
        普通用户建议使用其他数据源的实时行情。
        
        Args:
            stock_code: 股票代码
            
        Returns:
            实时行情数据字典，失败返回 None
        """
        # Tushare 实时行情需要高积分，普通用户无法使用
        # 这里仅作为接口预留，实际应使用 efinance 或 akshare 的实时数据
        logger.debug(f"Tushare 实时行情接口需要高积分，建议使用其他数据源: {stock_code}")
        return None

    # ==================== 扩展功能：大盘指数 ====================
    
    # 主要指数代码映射
    INDEX_CODES = {
        '000001.SH': '上证指数',
        '399001.SZ': '深证成指',
        '399006.SZ': '创业板指',
        '000688.SH': '科创50',
        '000016.SH': '上证50',
        '000300.SH': '沪深300',
    }
    
    def get_index_daily(self, trade_date: Optional[str] = None) -> List[Dict]:
        """
        获取主要指数日线行情
        
        使用 index_daily 接口获取指数行情
        文档：https://tushare.pro/document/2?doc_id=95
        
        Args:
            trade_date: 交易日期，格式 YYYYMMDD，默认为最近交易日
            
        Returns:
            指数行情列表，每个元素包含 code, name, current, change, change_pct 等
        """
        if self._api is None:
            logger.warning("Tushare API 未初始化，无法获取指数行情")
            return []
        
        results = []
        
        # 如果没有指定日期，获取最近交易日
        if trade_date is None:
            trade_date = datetime.now().strftime('%Y%m%d')
        else:
            trade_date = trade_date.replace('-', '')
        
        try:
            for ts_code, name in self.INDEX_CODES.items():
                try:
                    # 获取最近的行情数据
                    df = self._api.index_daily(
                        ts_code=ts_code,
                        start_date=trade_date,
                        end_date=trade_date
                    )
                    
                    # 如果当天没数据，尝试获取最近5天
                    if df is None or df.empty:
                        end_date = trade_date
                        start_date = (datetime.strptime(trade_date, '%Y%m%d') - 
                                     pd.Timedelta(days=10)).strftime('%Y%m%d')
                        df = self._api.index_daily(
                            ts_code=ts_code,
                            start_date=start_date,
                            end_date=end_date
                        )
                    
                    if df is not None and not df.empty:
                        # 取最新一条
                        row = df.iloc[0]
                        results.append({
                            'code': ts_code,
                            'name': name,
                            'current': float(row.get('close', 0)),
                            'change': float(row.get('change', 0)),
                            'change_pct': float(row.get('pct_chg', 0)),
                            'open': float(row.get('open', 0)),
                            'high': float(row.get('high', 0)),
                            'low': float(row.get('low', 0)),
                            'pre_close': float(row.get('pre_close', 0)),
                            'volume': float(row.get('vol', 0)) * 100,  # 手转股
                            'amount': float(row.get('amount', 0)) * 1000,  # 千元转元
                        })
                        
                except Exception as e:
                    logger.warning(f"[Tushare] 获取指数 {ts_code} 失败: {e}")
                    continue
            
            logger.info(f"[Tushare] 成功获取 {len(results)} 个指数行情")
            return results
            
        except Exception as e:
            logger.error(f"[Tushare] 获取指数行情失败: {e}")
            return []

    # ==================== 扩展功能：市场统计 ====================
    
    def get_market_daily_stats(self, trade_date: Optional[str] = None) -> Dict:
        """
        获取市场每日涨跌统计
        
        使用 daily_basic 接口获取每日指标，统计涨跌家数
        文档：https://tushare.pro/document/2?doc_id=32
        
        Args:
            trade_date: 交易日期，格式 YYYYMMDD
            
        Returns:
            市场统计数据，包含 up_count, down_count, flat_count, limit_up, limit_down, total_amount
        """
        if self._api is None:
            logger.warning("Tushare API 未初始化，无法获取市场统计")
            return {}
        
        if trade_date is None:
            trade_date = datetime.now().strftime('%Y%m%d')
        else:
            trade_date = trade_date.replace('-', '')
        
        try:
            logger.info(f"[Tushare] 调用 daily_basic 获取 {trade_date} 市场统计...")
            
            # 获取每日指标（包含涨跌幅）
            df = self._api.daily_basic(
                trade_date=trade_date,
                fields='ts_code,close,pct_chg,turnover_rate,total_mv,circ_mv'
            )
            
            if df is None or df.empty:
                # 尝试获取前一个交易日
                logger.warning(f"[Tushare] {trade_date} 无数据，尝试获取最近交易日...")
                
                # 获取交易日历
                cal_df = self._api.trade_cal(
                    exchange='SSE',
                    start_date=(datetime.strptime(trade_date, '%Y%m%d') - 
                               pd.Timedelta(days=10)).strftime('%Y%m%d'),
                    end_date=trade_date,
                    is_open='1'
                )
                
                if cal_df is not None and not cal_df.empty:
                    last_trade_date = cal_df.iloc[-1]['cal_date']
                    df = self._api.daily_basic(
                        trade_date=last_trade_date,
                        fields='ts_code,close,pct_chg,turnover_rate,total_mv,circ_mv'
                    )
            
            if df is None or df.empty:
                logger.warning("[Tushare] daily_basic 返回空数据")
                return {}
            
            # 统计涨跌
            df['pct_chg'] = pd.to_numeric(df['pct_chg'], errors='coerce')
            
            up_count = len(df[df['pct_chg'] > 0])
            down_count = len(df[df['pct_chg'] < 0])
            flat_count = len(df[df['pct_chg'] == 0])
            
            # 涨停跌停（涨跌幅 >= 9.9% 或 <= -9.9%）
            limit_up = len(df[df['pct_chg'] >= 9.9])
            limit_down = len(df[df['pct_chg'] <= -9.9])
            
            # 总市值（亿元）
            df['total_mv'] = pd.to_numeric(df['total_mv'], errors='coerce')
            total_mv = df['total_mv'].sum() / 1e8 if 'total_mv' in df.columns else 0
            
            result = {
                'trade_date': trade_date,
                'up_count': up_count,
                'down_count': down_count,
                'flat_count': flat_count,
                'limit_up_count': limit_up,
                'limit_down_count': limit_down,
                'total_count': len(df),
                'total_mv': total_mv,
            }
            
            logger.info(f"[Tushare] 市场统计: 涨:{up_count} 跌:{down_count} 平:{flat_count} "
                       f"涨停:{limit_up} 跌停:{limit_down}")
            
            return result
            
        except Exception as e:
            logger.error(f"[Tushare] 获取市场统计失败: {e}")
            return {}

    @property
    def is_available(self) -> bool:
        """检查 Tushare API 是否可用"""
        return self._api is not None


if __name__ == "__main__":
    # 测试代码
    logging.basicConfig(level=logging.DEBUG)
    
    fetcher = TushareFetcher()
    
    try:
        # 测试历史数据
        df = fetcher.get_daily_data('600519')  # 茅台
        print(f"获取成功，共 {len(df)} 条数据")
        print(df.tail())
        
        # 测试股票名称
        name = fetcher.get_stock_name('600519')
        print(f"股票名称: {name}")
        
    except Exception as e:
        print(f"获取失败: {e}")
