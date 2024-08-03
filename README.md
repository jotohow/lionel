# lionel

## Description
lionel is a [Fantasy Premier League]([url](https://fantasy.premierleague.com/)) team picking package. It forecasts FPL points using four models and maximises those points subject to the FPL constraints.

### Models
- Naive: Predicts using the player's last points tally
- LGBM_lag: LightGBM using lagged points tallies
- LGBM_exog: LightGBM using lagged points tallies and future exogenous variables (those known in advnce: own team, opponent team, position)
- LSTM: A LSTM model incorporating lagged points, future exogenous variables and historical exogenous variables (e.g. previous goals scored, bonus points)

## Install it from PyPI
**In prog**
```bash
pip install project_name
```

## Usage
**In prog**
```py
from project_name import BaseClass
from project_name import base_function

BaseClass().base_method()
base_function()
```

```bash
$ python -m project_name
#or
$ project_name
```

## Development
**In prog**
Read the [CONTRIBUTING.md](CONTRIBUTING.md) file.