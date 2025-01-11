# Lionel

**Lionel** is a Fantasy Premier League (FPL) team optimization tool that uses Bayesian hierarchical modeling and linear programming to predict and maximize expected points.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
  - [Predicting Expected Points](#predicting-expected-points)
  - [Selecting an Optimal Team](#selecting-an-optimal-team)
- [Data Sources](#data-sources)
- [Web Application](#web-application)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)
- [Acknowledgements](#acknowledgements)

## Features

- **Bayesian Hierarchical Modeling:** Predict player performance using PyMC's No-U-Turn Sampler.
- **Linear Programming Optimization:** Optimize team selection within budget and positional constraints using PuLP.
- **Pipeline Management:** Handle data workflows with Luigi.
- **Web Interface:** Access team selections and model details via a web application.
- **Open Source:** Contributions welcomed!

## Installation

Lionel can be installed using `pip` or by cloning the repository.

### Using `pip`

```bash
pip install git+https://github.com/jth500/lionel.git
```

### Cloning the Repository

```bash
git clone https://github.com/jth500/lionel.git
cd lionel
pip install -r requirements.txt
```

## Usage

### Predicting Expected Points

Use the `FPLPointsModel` to build and fit the Bayesian hierarchical model.

```python
import pandas as pd
from lionel.model.hierarchical import FPLPointsModel

# Sample data
data = {
    'player': ["player_1", "player_2", "player_3", "player_4", "player_5", "player_6"] * 2,
    'gameweek': [1]*6 + [2]*6,
    'season': [25]*12,
    'home_team': ["team_1"]*6 + ["team_2"]*6,
    'away_team': ["team_2"]*6 + ["team_1"]*6,
    'home_goals': [1]*6 + [2]*6,
    'away_goals': [0]*6 + [1]*6,
    'position': ["FWD", "MID", "DEF", "GK", "FWD", "MID"]*2,
    'minutes': [90]*12,
    'goals_scored': [1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 1],
    'assists': [0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1],
    'is_home': [True, True, True, False, False, False]*2,
    'points': [10, 6, 2, 2, 2, 2, 6, 2, 10, 10, 2, 10]
}

df = pd.DataFrame(data)

# Initialize and fit the model
fpl_model = FPLPointsModel()
fpl_model.build_model(df, df['points'])
fpl_model.fit(df, df['points'])

# Predict expected points
predictions = fpl_model.predict(df, extend_idata=False, combined=False, predictions=True)
print(predictions)
```

**Output:**
```
[8.1046909 , 4.69126912, 6.41898185, 8.3587504 , 2.86888174, 6.79323443,
 7.11748533, 3.33156234, 4.50141007, 6.13532871, 1.75916264, 5.61056724]
```

### Selecting an Optimal Team

Use `NewXVSelector` and `XISelector` to select the starting XI and bench players.

```python
import pandas as pd
import numpy as np
from lionel.selector import NewXVSelector, XISelector

# Sample data
df = pd.DataFrame({
    'player': [f"player_{i}" for i in range(20)],
    'team_name': [f'team_{i%10}' for i in range(20)],
    'position': ["FWD"]*3 + ["MID"]*8 + ["DEF"]*7 + ["GK"]*2,
    'value': [100, 90, 55, 45, 100, 90, 55, 45, 100, 90, 55, 45, 100, 90, 55, 45, 100, 90, 55, 45],
    'points_pred': np.random.normal(5, 2, 20),
    'xv': [0]*20,
    'xi': [0]*20,
    'captain': [0]*20,
})

# Select starting XI
xv_selector = NewXVSelector('points_pred')
xv_selector.build_problem(df, budget=1000)
xv_selector.solve()

# Select bench players
xi_selector = XISelector("points_pred")
xi_selector.build_problem(xv_selector.data)
xi_selector.solve()

# Display selected players
selected_players = xi_selector.data[xi_selector.data.xv == 1]
print(selected_players)
```

**Sample Output:**

| player    | team_name | position | value | points_pred | xv | xi | captain |
|-----------|-----------|----------|-------|-------------|----|----|---------|
| player_0  | team_0    | FWD      | 100   | 4.55        | 1  | 1  | 0       |
| player_1  | team_1    | FWD      | 90    | 4.95        | 1  | 1  | 0       |
| player_2  | team_2    | FWD      | 55    | 7.78        | 1  | 1  | 0       |
| player_3  | team_3    | MID      | 45    | 4.96        | 1  | 1  | 0       |
| player_5  | team_5    | MID      | 90    | 4.55        | 1  | 1  | 0       |
| player_7  | team_7    | MID      | 45    | 3.11        | 1  | 0  | 0       |
| player_9  | team_9    | MID      | 90    | 4.90        | 1  | 1  | 0       |
| player_10 | team_0    | MID      | 55    | 5.30        | 1  | 1  | 0       |
| player_11 | team_1    | DEF      | 45    | 2.45        | 1  | 0  | 0       |
| player_13 | team_3    | DEF      | 90    | 6.90        | 1  | 1  | 0       |
| player_14 | team_4    | DEF      | 55    | 10.78       | 1  | 1  | 1       |
| player_15 | team_5    | DEF      | 45    | 3.83        | 1  | 0  | 0       |
| player_17 | team_7    | DEF      | 90    | 6.71        | 1  | 1  | 0       |
| player_18 | team_8    | GK       | 55    | 3.96        | 1  | 0  | 0       |
| player_19 | team_9    | GK       | 45    | 7.77        | 1  | 1  | 0       |

## Data Sources

- **Fantasy Premier League (Pre-2024/25):** [Vaastav/Fantasy-Premier-League](https://github.com/vaastav/Fantasy-Premier-League)
- **Betting Odds:** [The Odds API](https://the-odds-api.com)

## Web Application

Access the web application for team selections and model information:

[https://lionel.streamlit.app/](https://lionel.streamlit.app/)

## Contributing

Contributions are welcome. Please follow the guidelines below.

### How to Contribute

1. **Fork the Repository**
   - Click the **Fork** button on the repository page.

2. **Clone Your Fork**
   ```bash
   git clone https://github.com/your-username/lionel.git
   cd lionel
   ```

3. **Create a New Branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

4. **Make Changes**
   - Implement your feature or fix.

5. **Commit Changes**
   ```bash
   git commit -m "Add feature: description"
   ```

6. **Push to Fork**
   ```bash
   git push origin feature/your-feature-name
   ```

7. **Create a Pull Request**
   - Navigate to the original repository and open a pull request.

### Code of Conduct

Please adhere to the [Code of Conduct](CODE_OF_CONDUCT.md).

## License

This project is licensed under the [MIT License](LICENSE).

## Contact

- **Email:** jtobyh@gmail.com
- **GitHub Issues:** [Issues Page](https://github.com/jth500/lionel/issues)

## Acknowledgements

- **[PyMC](https://www.pymc.io/welcome.html):** Bayesian modeling.
- **[PuLP](https://coin-or.github.io/pulp/):** Linear programming.
- **[Luigi](https://github.com/spotify/luigi):** Pipeline orchestration.
- **[Vaastav/Fantasy-Premier-League](https://github.com/vaastav/Fantasy-Premier-League):** FPL data.
- **[The Odds API](https://the-odds-api.com):** Betting odds data.

---
