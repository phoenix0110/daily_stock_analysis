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
from config import get_config

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
    priority = 2
    
    def __init__(self):
        """初始化 TushareFetcher"""
        self._api: Optional[object] = None  # Tushare API 实例
        
        # 尝试初始化 API
        self._init_api()
    
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
            http_url = config.tushare_http_url
            
            # 调试日志：打印 token 前10位和后4位（隐藏中间部分）
            if token:
                token_preview = f"{token[:10]}...{token[-4:]}" if len(token) > 14 else token[:4] + "..."
                logger.info(f"[Tushare] Token: {token_preview}, HTTP_URL: {http_url or '默认'}")
            
            # 获取 API 实例（传入 token）
            self._api = ts.pro_api(token)
            
            # 如果配置了自定义服务端点，需要额外设置
            if http_url:
                # 强制设置 token 和 http_url（某些 Tushare 服务需要）
                self._api._DataApi__token = token
                self._api._DataApi__http_url = http_url
                logger.info(f"Tushare API 初始化成功（自定义端点: {http_url}）")
            else:
                logger.info("Tushare API 初始化成功（标准模式）")
            
        except Exception as e:
            logger.error(f"Tushare API 初始化失败: {e}")
            self._api = None
    
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

    # ==================== 扩展功能：股票名称 ====================
    
    def get_all_stock_names(self) -> Dict[str, str]:
        """
        获取所有A股股票名称映射
        
        使用 stock_basic 接口获取所有上市股票的代码和名称
        文档：https://tushare.pro/document/2?doc_id=25
        
        Returns:
            Dict[str, str]: {股票代码: 股票名称} 映射，如 {'000001': '平安银行'}
        """
        if self._api is None:
            logger.warning("Tushare API 未初始化，无法获取股票名称")
            return {}
        
        try:
            logger.info("[Tushare] 调用 stock_basic 获取所有股票名称...")
            
            # 获取所有上市股票（L=上市）
            df = self._api.stock_basic(
                exchange='',
                list_status='L',
                fields='ts_code,symbol,name,industry'
            )
            
            if df is None or df.empty:
                logger.warning("[Tushare] stock_basic 返回空数据")
                return {}
            
            # 构建映射：symbol (6位代码) -> name
            stock_map = {row['symbol']: row['name'] for _, row in df.iterrows()}
            
            logger.info(f"[Tushare] 成功获取 {len(stock_map)} 只股票的名称")
            return stock_map
            
        except Exception as e:
            logger.error(f"[Tushare] 获取股票名称失败: {e}")
            return {}
    
    def get_stock_name(self, stock_code: str) -> Optional[str]:
        """
        获取单只股票的名称
        
        Args:
            stock_code: 股票代码（6位数字）
            
        Returns:
            股票名称，获取失败返回 None
        """
        if self._api is None:
            return None
        
        try:
            ts_code = self._convert_stock_code(stock_code)
            
            df = self._api.stock_basic(
                ts_code=ts_code,
                fields='ts_code,symbol,name'
            )
            
            if df is not None and not df.empty:
                return df.iloc[0]['name']
            
            return None
            
        except Exception as e:
            logger.error(f"[Tushare] 获取 {stock_code} 名称失败: {e}")
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
        df = fetcher.get_daily_data('600519')  # 茅台
        print(f"获取成功，共 {len(df)} 条数据")
        print(df.tail())
    except Exception as e:
        print(f"获取失败: {e}")
