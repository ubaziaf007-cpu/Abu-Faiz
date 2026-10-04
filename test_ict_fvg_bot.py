import unittest, tempfile, os
from ict_fvg_bot import *

DAY = 86_400_000


def C(t, o, h, l, c):
    return Candle(t, o, h, l, c)


class Tests(unittest.TestCase):
    def test_fvg(self):
        cs = [C(0, 10, 11, 9, 10), C(1, 10, 15, 10, 14), C(2, 14, 16, 12, 15)]
        z = fvg_at(cs, 2)
        self.assertEqual((z.kind, z.low, z.high), (BULL, 11, 12))
        cs = [C(0, 10, 11, 9, 10), C(1, 10, 10, 5, 6), C(2, 6, 8, 5, 7)]
        z = fvg_at(cs, 2)
        self.assertEqual((z.kind, z.low, z.high), (BEAR, 8, 9))

    def test_inversion_needs_body(self):
        z = Zone(BEAR, 8, 9, 2)
        self.assertFalse(inverts(z, C(0, 8.5, 12, 7, 8.8)))      # wick through, close inside
        self.assertFalse(inverts(z, C(0, 8.5, 12, 8, 9.5), True))  # opens inside gap: not strict
        self.assertTrue(inverts(z, C(0, 8.5, 12, 8, 9.5), False))
        self.assertTrue(inverts(z, C(0, 7.5, 12, 7, 9.5)))
        zb = Zone(BULL, 11, 12, 2)
        self.assertTrue(inverts(zb, C(0, 12.5, 13, 9, 10.5)))

    def test_swings_and_votes(self):
        wave = [0, 3, 6, 3, 0, 3, 6, 3]
        up = [C(i, 10 + i + wave[i % 8] / 2, 11 + i + wave[i % 8], 9 + i + wave[i % 8] - 6, 10 + i + wave[i % 8] / 2)
              for i in range(40)]
        self.assertEqual(vote_structure(up), BULL)
        down = [C(i, -c.o, -c.l, -c.h, -c.c) for i, c in enumerate(up)]
        self.assertEqual(vote_structure(down), BEAR)
        self.assertEqual(vote_pdh_pdl([C(0, 5, 10, 4, 6)], 11), BULL)
        self.assertEqual(vote_pdh_pdl([C(0, 5, 10, 4, 6)], 3), BEAR)
        self.assertEqual(vote_pdh_pdl([C(0, 5, 10, 4, 6)], 6), 0)
        d = [C(i, 10, 12, 8, 10) for i in range(6)] + [C(6, 10, 11, 7, 10)]
        self.assertEqual(vote_sweep(d), BULL)
        d = [C(i, 10, 12, 8, 10) for i in range(6)] + [C(6, 10, 13, 9, 10)]
        self.assertEqual(vote_sweep(d), BEAR)

    def test_full_pipeline_long(self):
        day = 100 * DAY
        c15 = [C(day - 30 * 15 * MIN + k * 15 * MIN, 100, 101, 99, 100) for k in range(30)]
        # sweep candle at day+0: wick to 95, closes 100
        c15.append(C(day, 100, 100.5, 95, 100))
        c5, t = [], day - 2 * 5 * MIN
        def add(o, h, l, c):
            nonlocal t
            c5.append(C(t, o, h, l, c)); t += 5 * MIN
        add(100, 100, 99, 99.5); add(99.5, 99.6, 98.5, 98.8)      # pre-sweep filler
        add(98.5, 99, 96, 96.5)                                    # c1 (sweep window)
        add(96.5, 96.6, 95, 95.5)                                  # c2 big drop
        add(95.5, 97, 95.2, 96.9)                                  # c3: high 97 < c1.low 96? no
        # make a clean bearish FVG: c1.low=96 > c3.high  -> c3 high must be <96
        c5[-1] = C(c5[-1].t, 95.5, 95.8, 95.2, 95.6)               # bearish FVG zone [95.8, 96]
        add(95.6, 97.5, 95.5, 97.2)                                # body close through (open<=95.8,close>=96) -> IFVG
        add(97.2, 97.3, 96.5, 96.7)
        add(96.7, 97.0, 95.9, 97.0)                                # retest zone high 96, rejection: close>96, close>open
        for _ in range(20): add(97, 97.5, 96.9, 97.2)
        p = Params(market=CRYPTO, sweep_lookback=24, pip=0.0001)
        su = find_setups(c15, c5, day, day + DAY, BULL, p)
        self.assertEqual(len(su), 1)
        s = su[0]
        self.assertEqual(s.entry, 97.0)
        self.assertLess(s.stop, 95)
        self.assertAlmostEqual((s.target - s.entry) / (s.entry - s.stop), 3.0)
        self.assertEqual(find_setups(c15, c5, day, day + DAY, BEAR, p), [])   # never against bias
        self.assertEqual(find_setups(c15, c5, day, day + DAY, 0, p), [])

    def test_simulate(self):
        s = Setup(BULL, 0, 100, 98, 106, 98.5, 0)
        mk = lambda hi, lo: [C(5 * MIN, 100, hi, lo, 100)]
        self.assertEqual(simulate(s, mk(107, 99), DAY), (3.0, "TP"))
        self.assertEqual(simulate(s, mk(107, 97), DAY)[1], "SL")

    def test_forex_day_boundaries(self):
        u = lambda *a: int(datetime(*a, tzinfo=UTC).timestamp() * 1000)
        # winter: NY day opens 17:00 EST = 22:00Z; Monday midday belongs to the day opened Sunday
        self.assertEqual(FOREX.day_start(u(2026, 1, 12, 12)), u(2026, 1, 11, 22))
        # summer: 17:00 EDT = 21:00Z
        self.assertEqual(FOREX.day_start(u(2026, 7, 14, 12)), u(2026, 7, 13, 21))
        # the instant of the open starts the new day, one ms before belongs to the old one
        self.assertEqual(FOREX.day_start(u(2026, 1, 12, 22)), u(2026, 1, 12, 22))
        self.assertEqual(FOREX.day_start(u(2026, 1, 12, 22) - 1), u(2026, 1, 11, 22))
        # DST spring-forward day is 23h long
        st = u(2026, 3, 7, 22)
        self.assertEqual(FOREX.day_end(st) - st, 23 * HOUR)

    def test_killzone(self):
        u = lambda *a: int(datetime(*a, tzinfo=UTC).timestamp() * 1000)
        self.assertTrue(FOREX.in_killzone(u(2026, 1, 12, 8)))     # 03:00 NY
        self.assertFalse(FOREX.in_killzone(u(2026, 1, 12, 11)))   # 06:00 NY
        self.assertTrue(FOREX.in_killzone(u(2026, 1, 12, 13)))    # 08:00 NY
        self.assertTrue(CRYPTO.in_killzone(u(2026, 1, 12, 11)))

    def test_weekend_dropped_and_resample(self):
        u = lambda *a: int(datetime(*a, tzinfo=UTC).timestamp() * 1000)
        c5 = []
        t = u(2026, 1, 12, 22)                       # Monday-session day start
        for k in range(288):
            c5.append(C(t + k * 5 * MIN, 1, 2, 0.5, 1.5))
        c5.append(C(u(2026, 1, 16, 22) + 5 * 60 * 60_000 * 0, 1, 1, 1, 1))   # lone Friday-evening stub
        d = to_daily(c5, FOREX)
        self.assertEqual(len(d), 1)
        m15 = to_15m(c5[:7])
        self.assertEqual(len(m15), 3)                 # 7 candles -> buckets of 3,3,1
        self.assertEqual(len(to_15m(c5[:7], until=c5[6].t + 5 * MIN)), 2)  # forming bucket dropped

    def test_csv_mt5_and_forex_costs(self):
        d = tempfile.mkdtemp()
        f = os.path.join(d, "m5.csv")
        open(f, "w").write("<DATE>\t<TIME>\t<OPEN>\t<HIGH>\t<LOW>\t<CLOSE>\n2026.01.12\t10:05\t1.1\t1.2\t1.0\t1.15\n2026.01.12\t10:00\t1.0\t1.1\t0.9\t1.1\n")
        cs = load_csv(f)
        self.assertEqual([c.c for c in cs], [1.1, 1.15])
        self.assertEqual(cs[0].t, int(datetime(2026, 1, 12, 10, tzinfo=UTC).timestamp() * 1000))
        self.assertEqual(pip_size("USD_JPY"), 0.01)
        self.assertEqual(pip_size("EUR_USD"), 0.0001)
        # forex stop buffer = pips, min-risk filter
        p = Params(market=FOREX, pip=0.0001)
        self.assertAlmostEqual(p.buffer(1.1), 0.0002)
        self.assertAlmostEqual(p.min_risk(), 0.0005)

    def test_lock_time_and_h4(self):
        u = lambda *a: int(datetime(*a, tzinfo=UTC).timestamp() * 1000)
        self.assertEqual(FOREX.lock_time(u(2026, 1, 11, 22)), u(2026, 1, 12, 7))      # 02:00 EST
        self.assertEqual(FOREX.lock_time(u(2026, 7, 13, 21)), u(2026, 7, 14, 6))      # 02:00 EDT
        c5 = [C(u(2026, 1, 11, 22) + k * 5 * MIN, 1, 2, 0.5, 1.5) for k in range(96)]  # 8h
        h4 = to_h4(c5, FOREX)
        self.assertEqual([c.t for c in h4], [u(2026, 1, 11, 22), u(2026, 1, 12, 2)])

    def test_lock_bias_has_no_lookahead(self):
        u = lambda *a: int(datetime(*a, tzinfo=UTC).timestamp() * 1000)
        ds = u(2026, 1, 11, 22)
        # 12 prior days with ranges 1.00-1.10, then today
        daily = [C(ds - (12 - k) * DAY, 1.05, 1.10, 1.00, 1.05) for k in range(12)]
        c5 = [C(ds + k * 5 * MIN, 1.05, 1.06, 1.04, 1.05) for k in range(12 * 9 + 1)]   # up to lock + 5m
        h4 = to_h4(c5, FOREX)
        p = Params(market=FOREX)
        before = lock_bias(daily, h4, c5, ds, p)
        lock_t = FOREX.lock_time(ds)
        # rewrite everything at/after the lock with a huge spike: result must not change
        c5b = [c if c.t < lock_t else C(c.t, 9, 9, 9, 9) for c in c5]
        after = lock_bias(daily, to_h4(c5b, FOREX), c5b, ds, p)
        self.assertEqual(before[:2], after[:2])
        self.assertEqual(before[1]["A_pdh_pdl"], 0)                    # price 1.05 inside yesterday's range


if __name__ == "__main__":
    unittest.main()
