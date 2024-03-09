from scrape.managers.budgeter import Budgeter


def test_budgeter(picks):
    budgeter = Budgeter(picks)
    a = budgeter[263]
    assert len(budgeter.picks) == 15
    assert a == {"element": 263, "selling_price": 45, "purchase_price": 45}
    assert budgeter.get_sale_price(263) == 45
