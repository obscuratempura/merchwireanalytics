from app.logic.ranking import BrandSignal, rank_brands, top_sku_movers, MoverEntry


def test_rank_brands():
    signals = [
        BrandSignal(1, "A", 0.2, 5, 1.0),
        BrandSignal(2, "B", 0.1, 1, 0.0),
    ]
    leaderboard = rank_brands(signals)
    assert leaderboard[0].brand_name == "A"
    assert leaderboard[0].rank == 1


def test_top_sku_movers():
    movers = [
        MoverEntry(1, "A", "Prod", "SKU", 100, 80, 0.25, 0.2),
        MoverEntry(2, "B", "Prod2", "SKU2", 90, 80, 0.05, 0.1),
    ]
    result = top_sku_movers(movers)
    assert len(result) == 2
    assert result[0].brand_name == "A"
