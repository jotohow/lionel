# lionel

## Description
lionel is a [Fantasy Premier League]([url](https://fantasy.premierleague.com/)) team picking package. It forecasts FPL points using four models and maximises those points subject to the FPL constraints.

### Models
- Naive: Predicts using the player's last points tally
- LGBM_lag: LightGBM using lagged points tallies
- LGBM_exog: LightGBM using lagged points tallies and future exogenous variables (those known in advnce: own team, opponent team, position)
- LSTM: A LSTM model incorporating lagged points, future exogenous variables and historical exogenous variables (e.g. previous goals scored, bonus points)


### Optimisation
- Uses the [PuLP]([url](https://coin-or.github.io/pulp/)) package for linear programming to maximise forecasted points subject to the budget, team, and position constraints. 
- Picks a first XV, selects the first XI from that, and chooses a captain who receives double points.

## Usage
```py
from lionel.run import run
team_selection = run(season=24, next_gw=32, gw_horizon=5)
```

```bash
$ python -m lionel.run 24 32 5
```

## Development
Read the [CONTRIBUTING.md](CONTRIBUTING.md) file.
