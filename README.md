# lionel
A Fantasy Premier League team optimisation tool that uses Bayesian hierarchical modelling ([PyMC](https://www.pymc.io/welcome.html)) and linear programming ([PuLP](https://coin-or.github.io/pulp/)) to maximise expected points.



## Index

- [About](#about)
- [Usage](#usage)
  - [Installation](#installation)
  - [Commands](#commands)
- [Data](#Data)
- [License](#license)

## About
Lionel predicts Fantasy Premier League (FPL) points using a Bayesian hierarchical model, implemented with a No-U-Turn Sampler in PyMC. Then, it maximises the predicted points using linear programming as implemented in PuLP. More information about the model and its outputs can be found on this [web app](https://lionel.streamlit.app/).

## Usage

### Installation

Either pip install from git:
```
pip install git+https://github.com/jth500/lionel.git
```

Or clone the repository.

### Example usage

Using FPLPointsModel to get expected points
```
import pandas as pd
import numpy as np
import arviz as az

from lionel.model.hierarchical import FPLPointsModel

player = ["player_1", "player_2", "player_3", "player_4", "player_5", "player_6"] 
gameweek = [1] * len(player) + [2] * len(player)
season = [25] * len(player) * 2
home_team = ["team_1"] * len(player) + ["team_2"] * len(player)
away_team = ["team_2"] * len(player) + ["team_1"] * len(player)
home_goals = [1] * len(player) + [2] * len(player)
away_goals = [0] * len(player) + [1] * len(player)
position = ["FWD", "MID", "DEF", "GK", "FWD", "MID"] * 2
minutes = [90] * len(player) * 2
goals_scored = [1, 0, 0, 0, 0, 0] + [0, 0, 1, 1, 0, 1]
assists = [0, 1, 0, 0, 0, 0] + [1, 0, 0, 0, 1, 1]
is_home = [True, True, True, False, False, False] + [False, False, False, True, True, True]
points = [10, 6, 2, 2, 2, 2] + [6, 2, 10, 10, 2, 10]

df = pd.DataFrame({
    'player': player + player, 
    'gameweek': gameweek, 
    'season': season, 
    'home_team': home_team, 
    'away_team': away_team, 
    'home_goals': home_goals, 
    'away_goals': away_goals, 
    'position': position, 
    'minutes': minutes, 
    'goals_scored': goals_scored, 
    'assists': assists, 
    'is_home': is_home
})

fplm = FPLPointsModel()
fplm.build_model(df, points)
fplm.fit(df, points)

# Get predicted points
fplm.predict(df, extend_idata=False, combined=False, predictions=True)

# array([8.1046909 , 4.69126912, 6.41898185, 8.3587504 , 2.86888174,6.79323443, 7.11748533, 3.33156234, 4.50141007, 6.13532871, 1.75916264, 5.61056724])
```

Using selector to select a Fantasy Team 

```
import pandas as pd
from lionel.selector import NewXVSelector, XISelector

df = pd.DataFrame({
    'player': [f"player_{i}" for i in range(20)],
    'team_name': [f'team_{i}' for i in range(10)] * 2,
    'position': ["FWD"] * 3 + ["MID"] * 8 + ["DEF"] * 7 + ["GK"] * 2,
    'value': [100, 90, 55, 45] * 5,
    'points_pred': np.random.normal(5, 2, 20),
    'xv': [0] * 20,
    'xi': [0] * 20,
    'captain': [0] * 20,
})

xv_sel = NewXVSelector('points_pred')
xv_sel.build_problem(df, budget=1000)
xv_sel.solve()


xi_sel = XISelector("points_pred")
xi_sel.build_problem(xv_sel.data)
xi_sel.solve()

xi_sel.data[xi_sel.data.xv==1]
```
Returns the following, which obeys the position, budget, and team constraints:
| player      | team_name | position | value | points_pred | xv | xi | captain |
|-------------|-----------|----------|-------|-------------|----|----|---------|
| player_0    | team_0    | FWD      | 100   | 4.550896    | 1  | 1  | 0       |
| player_1    | team_1    | FWD      | 90    | 4.953460    | 1  | 1  | 0       |
| player_2    | team_2    | FWD      | 55    | 7.780676    | 1  | 1  | 0       |
| player_3    | team_3    | MID      | 45    | 4.961371    | 1  | 1  | 0       |
| player_5    | team_5    | MID      | 90    | 4.548816    | 1  | 1  | 0       |
| player_7    | team_7    | MID      | 45    | 3.105254    | 1  | 0  | 0       |
| player_9    | team_9    | MID      | 90    | 4.897183    | 1  | 1  | 0       |
| player_10   | team_0    | MID      | 55    | 5.297736    | 1  | 1  | 0       |
| player_11   | team_1    | DEF      | 45    | 2.450868    | 1  | 0  | 0       |
| player_13   | team_3    | DEF      | 90    | 6.899006    | 1  | 1  | 0       |
| player_14   | team_4    | DEF      | 55    | 10.779401   | 1  | 1  | 1       |
| player_15   | team_5    | DEF      | 45    | 3.833575    | 1  | 0  | 0       |
| player_17   | team_7    | DEF      | 90    | 6.717436    | 1  | 1  | 0       |
| player_18   | team_8    | GK       | 55    | 3.969914    | 1  | 0  | 0       |
| player_19   | team_9    | GK       | 45    | 7.769195    | 1  | 1  | 0       |

## Data
FPL (Pre-2024/25): [Vaastav](https://github.com/vaastav/Fantasy-Premier-League)  
Betting: [The Odds API](https://the-odds-api.com)


##  License
MIT




