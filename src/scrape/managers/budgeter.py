class Budgeter:
    """
    List-like object for managing player cost info.

    Get sale price by player element with `Budgeter.get_sale_price(element)`.
    Get full info with `Budgeter[element]`.
    """

    def __init__(self, picks: dict):
        self.picks = picks

    @property
    def picks(self):
        return self._picks

    @picks.setter
    def picks(self, picks):
        keep_keys = ["element", "selling_price", "purchase_price"]
        picks = [{k: v for k, v in pick.items() if k in keep_keys} for pick in picks]
        self._picks = picks

    def __getitem__(self, element):
        return next((item for item in self.picks if item["element"] == element), None)

    def get_sale_price(self, element):
        return next(
            (
                item["selling_price"]
                for item in self.picks
                if item["element"] == element
            ),
            None,
        )


if __name__ == "__main__":
    import json
    import os
    from pathlib import Path

    data = (
        Path(os.path.dirname(os.path.realpath(__file__))).parent.parent.parent / "data"
    )
    with open(data / "my_team.json") as f:
        picks = json.load(f)["picks"]

    budgeter = Budgeter(picks)
    a = budgeter[263]
    assert budgeter.get_sale_price(263) == 45
    pass
