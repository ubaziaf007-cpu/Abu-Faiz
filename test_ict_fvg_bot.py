import unittest
from ict_fvg_bot import *


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
        p = Params(sweep_lookback=24)
        su = find_setups(c15, c5, day, BULL, p)
        self.assertEqual(len(su), 1)
        s = su[0]
        self.assertEqual(s.entry, 97.0)
        self.assertLess(s.stop, 95)
        self.assertAlmostEqual((s.target - s.entry) / (s.entry - s.stop), 3.0)
        self.assertEqual(find_setups(c15, c5, day, BEAR, p), [])   # never against bias
        self.assertEqual(find_setups(c15, c5, day, 0, p), [])

    def test_simulate(self):
        s = Setup(BULL, 0, 100, 98, 106, 98.5, 0)
        mk = lambda hi, lo: [C(5 * MIN, 100, hi, lo, 100)]
        self.assertEqual(simulate(s, mk(107, 99), DAY), (3.0, "TP"))
        self.assertEqual(simulate(s, mk(107, 97), DAY)[1], "SL")


if __name__ == "__main__":
    unittest.main()
