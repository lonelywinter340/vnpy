from datetime import time, timedelta
from vnpy.app.cta_strategy import (
    CtaTemplate,
    StopOrder,
    TickData,
    BarData,
    TradeData,
    OrderData,
    BarGenerator,
    ArrayManager,
)
import numpy as np
from vnpy.trader.constant import Interval


class DualThrustRefinedStrategy(CtaTemplate):
    """"""

    author = "JM"

    # 策略参数
    fixed_size = 1 # 开仓头寸
    k1 = 0.4
    k2 = 0.6
    period = 1 #小周期看一分钟
    large_period_hour = 4 #大周期看四小时线
    n = 3 #过去3天的K线计算上下轨
    filter_by_trend = 1 # 是否用趋势过滤策略
    dynamic_stop_loss_rate = 0.005 # 移动止损比例，高位回落比例达到该值自动出发止盈止损

    # Bar数据暂存
    bars = [] # 分钟bar
    day_bars = [] # 天级bar

    # 当天价格
    day_open = 0
    day_high = 0
    day_low = 0

    # 前N天K线计算的波动范围、上轨、下轨
    range = 0
    long_entry = 0
    short_entry = 0

    split_time = time(hour=6, minute=0)
    open_time = time(hour=6, minute=0)
    close_time = time(hour=4, minute=0)

    long_entered = False
    short_entered = False

    trend_ma_period = 144
    trend = 1 # 0-震荡 1-上涨 2-下跌

    # 记录开仓后的最高价（多单）或最低价（空单）
    session_high = 0
    session_low = 0

    parameters = ["period", "k1", "k2", "n", "fixed_size", "filter_by_trend", "dynamic_stop_loss_rate"]
    variables = ["range", "long_entry", "short_entry"]

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        """"""
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)

        self.bg = BarGenerator(self.on_bar, self.period, self.on_bar_period)
        self.am = ArrayManager()

        self.bg_large = BarGenerator(self.on_bar, self.large_period_hour, self.on_bar_large_period, Interval.HOUR)
        self.am_large = ArrayManager(size=self.trend_ma_period + 1)

        self.bars = []
        self.day_bars = []

    def on_init(self):
        """
        Callback when strategy is inited.
        """
        self.write_log("策略初始化")
        self.load_bar(10)

    def on_start(self):
        """
        Callback when strategy is started.
        """
        self.write_log("策略启动")

    def on_stop(self):
        """
        Callback when strategy is stopped.
        """
        self.write_log("策略停止")

    def on_tick(self, tick: TickData):
        """
        Callback of new tick data update.
        """
        self.bg.update_tick(tick)
        self.bg_large.update_tick(tick)

    def on_bar(self, bar: BarData):
        """
        Callback of new bar data update.
        """
        self.bg.update_bar(bar)
        self.bg_large.update_bar(bar)

    def on_bar_large_period(self, bar: BarData):
        """
        """
        am = self.am_large
        am.update_bar(bar)
        if not am.inited:
            return

        # 用均线定势
        #self.write_log('%d, %d' % (am.count , self.trend_ma_period))
        trend_ma = am.sma(self.trend_ma_period, array=True)
        if trend_ma[-1] > trend_ma[-2]:
            self.trend = 1
        else:
            self.trend = -1
        #self.write_log('%f, %f, %d' % (trend_ma[-1], trend_ma[-2], self.trend))

    def on_bar_period(self, bar: BarData):
        """
        Callback of new bar data update.
        """
        self.cancel_all()

        self.bars.append(bar)
        if len(self.bars) <= 2:
            return
        else:
            self.bars.pop(0)
        last_bar = self.bars[-2]

        if last_bar.datetime.time() < self.split_time and bar.datetime.time() >= self.split_time:
            self.day_bars.append((self.day_open, self.day_close, self.day_high, self.day_low))
            self.open_time = bar.datetime
            self.close_time = bar.datetime + timedelta(hours=22)
            if len(self.day_bars) >= self.n:
                hh = max([x[2] for x in self.day_bars])
                ll = min([x[3] for x in self.day_bars])
                hc = max([x[1] for x in self.day_bars])
                lc = min([x[1] for x in self.day_bars])
                self.range = max(hh - lc, hc - ll)
                if self.trend == 1:
                    self.long_entry = bar.open_price + self.k1 * self.range
                    self.short_entry = bar.open_price - self.k2 * self.range
                else:
                    self.long_entry = bar.open_price + self.k2 * self.range
                    self.short_entry = bar.open_price - self.k1 * self.range
                self.day_bars.pop(0)

            self.day_open = bar.open_price
            self.day_high = bar.high_price
            self.day_low = bar.low_price

            self.long_entered = False
            self.short_entered = False
        else:
            if not self.day_open:
                self.day_open = bar.open_price
            self.day_close = bar.close_price
            self.day_high = max(self.day_high, bar.high_price)
            self.day_low = min(self.day_low, bar.low_price)
            if self.long_entered:
                self.session_high = max(self.session_high, bar.high_price)
            if self.short_entered:
                self.session_low = min(self.session_low, bar.low_price)

        if not self.range:
            return

        if bar.datetime < self.close_time:
            if self.pos == 0:
                if bar.close_price > self.day_open:
                    if not self.long_entered:
                        if not self.filter_by_trend or (self.filter_by_trend and self.trend == 1):
                            self.buy(self.long_entry, self.fixed_size, stop=True)
                else:
                    if not self.short_entered:
                        if not self.filter_by_trend or (self.filter_by_trend and self.trend == -1):
                            self.short(self.short_entry, self.fixed_size, stop=True)

            elif self.pos > 0:
                self.long_entered = True
                #self.sell(self.short_entry, self.fixed_size, stop=True)
                self.session_high = bar.high_price
                # 移动止损，突破后最高股价回落一定比例就主动止损止盈
                stop_point = max(self.session_high * (1.0 - self.dynamic_stop_loss_rate), self.short_entry)
                self.sell(stop_point, self.fixed_size, stop=True)
                if not self.short_entered:
                    if not self.filter_by_trend or (self.filter_by_trend and self.trend == -1):
                        self.short(self.short_entry, self.fixed_size, stop=True)

            elif self.pos < 0:
                self.short_entered = True
                #self.cover(self.long_entry, self.fixed_size, stop=True)
                self.session_low = bar.low_price
                # 移动止损，突破后最高股价回落一定比例就主动止损止盈
                stop_point = min(self.session_low * (1.0 + self.dynamic_stop_loss_rate), self.long_entry)
                self.cover(stop_point, self.fixed_size, stop=True)
                if not self.long_entered:
                    if not self.filter_by_trend or (self.filter_by_trend and self.trend == 1):
                        self.buy(self.long_entry, self.fixed_size, stop=True)
        else:
            if self.pos > 0:
                self.sell(bar.close_price * 0.99, abs(self.pos))
            elif self.pos < 0:
                self.cover(bar.close_price * 1.01, abs(self.pos))

        self.put_event()

    def on_order(self, order: OrderData):
        """
        Callback of new order data update.
        """
        pass

    def on_trade(self, trade: TradeData):
        """
        Callback of new trade data update.
        """
        self.put_event()

    def on_stop_order(self, stop_order: StopOrder):
        """
        Callback of stop order update.
        """
        #self.write_log(stop_order)
        pass
