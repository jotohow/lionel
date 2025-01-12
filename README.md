# Lionel

**Lionel** is a Fantasy Premier League (FPL) team optimization tool that serves **two primary goals**:

1. **Predict fantasy player points** with 1) a library-agnostic framework, and 2) a concrete Bayesian hierarchical implementation.
2. **Offer an extendable selection/optimization framework** for building squads under FPL-style constraints (budget, positions, transfers, etc.).

You can use the existing selection classes (e.g., for picking a 15-player squad or 11-player lineup) or adapt the **underlying selection logic** to build new team selection strategies. The **Lionel models package** can be extended to build **custom Bayesian (or other) models** for player points, which you can then pair with the selection framework to optimize squads based on your own custom predictions.

---

## Table of Contents

- [Features](#features)
- [Installation](#installation)
  - [Using pip](#using-pip)
  - [Cloning the Repository](#cloning-the-repository)
- [Usage](#usage)
  - [Predicting Expected Points](#predicting-expected-points)
  - [Selecting an Optimal Team](#selecting-an-optimal-team)
  - [Extensibility for Custom Optimization](#extensibility-for-custom-optimization)
- [Examples](#examples)
- [Data Sources](#data-sources)
- [Web Application](#web-application)
- [Contributing](#contributing)
  - [How to Contribute](#how-to-contribute)
  - [Code of Conduct](#code-of-conduct)
- [License](#license)
- [Contact](#contact)
- [Acknowledgements](#acknowledgements)

---

## Features

- **Bayesian Hierarchical Modeling**  
  Predict player performance using PyMC’s No-U-Turn Sampler (or bring your own modeling approach).

- **Linear Programming Optimization**  
  Optimize team selection within budget, positional, and transfer constraints using PuLP.

- **Modular & Extendable**  
  - **Selectors**: A core `BaseSelector` class plus specialized selectors (XV, XI, Update XV, etc.) let you add custom constraints/objectives for new or unanticipated use cases.  
  - **Models**: Base Model classes (`LionelBaseModel`, `BaseBayesianModel`) that you can subclass to create new modeling approaches for predicting fantasy points.  

- **Web Interface**  
  Access team selections and model details via a web application.

- **Open Source**  
  Contributions are welcomed!

---

## Installation

### Using pip

The released version
```bash
pip install lionel
```

Latest development version
```bash
pip install git+https://github.com/jth500/lionel.git
```

### Cloning the Repository

```bash
git clone https://github.com/jth500/lionel.git
cd lionel
pip install -r requirements.txt
```

---

## Usage

### Predicting Expected Points

One core functionality is **predicting FPL points** using a Bayesian hierarchical model. You can use the provided classes—like `HierarchicalPointsModel`—which implements a Bayesian approach for modelling player contributions.

```python
import pandas as pd
import numpy as np
from lionel.model.bayesian.hierachical import HierarchicalPointsModel

# Example data (12 rows for 2 gameweeks)
player = ["player_1", "player_2", "player_3", "player_4", "player_5", "player_6"] 
gameweek = [1]*6 + [2]*6
season = [25]*12
home_team = ["team_1"]*6 + ["team_2"]*6
away_team = ["team_2"]*6 + ["team_1"]*6
home_goals = [1]*6 + [2]*6
away_goals = [0]*6 + [1]*6
position = ["FWD","MID","DEF","GK","FWD","MID"]*2
minutes = [90]*12
goals_scored = [1,0,0,0,0,0,0,0,1,1,0,1]
assists = [0,1,0,0,0,0,1,0,0,0,1,1]
is_home = [True,True,True,False,False,False,False,False,False,True,True,True]
points = [10,6,2,2,2,2,6,2,10,10,2,10]

df = pd.DataFrame({
    'player': player+player,
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

# Initialize and fit the model
model = HierarchicalPointsModel()
model.fit(df, np.array(points), progressbar=True)

# Generate posterior predictive samples
preds = model.predict(df, predictions=True)
print(preds)
# np.array([5, 4, 1, ...])
```

### Selecting an Optimal Team

Another main feature is **team selection**. The project provides several specialized selectors, each inheriting from a base optimization class:

- **`XVSelector`**: Pick a 15-player squad (budget, max from each team, positional constraints, plus 1 captain).  
- **`XISelector`**: From an existing 15-player squad, select the best 11.  
- **`UpdateXVSelector`**: Make a limited number of transfers to an existing 15-player squad.

#### Example (Selecting XV)

```python
import pandas as pd
from lionel.selector.fpl.xv_selector import XVSelector 

data = [
    {"player_id": 1, "name": "Player A1", "team": "Arsenal", "position": "GK", "price": 5.0, "predicted_points": 50},
    # ... more players ...
]
candidate_df = pd.DataFrame(data)

# Instantiate XVSelector
selector = XVSelector(candidate_df, pred_var="predicted_points", budget=1000)
optimal_squad = selector.select()

print("Selected 15-player squad:")
print(optimal_squad[optimal_squad["xv"] == 1])
print("\nCaptain chosen:")
print(optimal_squad[optimal_squad["captain"] == 1])
```

### Extensibility for Custom Optimization

Under the hood, these selectors use a **`BaseSelector`** that:

- Defines binary decision variables for each player  
- Sets an objective function (e.g., maximize total predicted points)  
- Applies constraints (budget, positions, etc.)  
- Solves the optimization with PuLP or another solver

You can **create your own selectors** or **custom constraints** by subclassing `BaseSelector`. For instance, you might:

- Set an entirely different objective (e.g., risk-adjusted points).  
- Enforce different constraints (e.g., “must include at least 2 defenders from Team X”).  

This modular design makes it straightforward to plug in new logic without rewriting the entire optimization code.

---

## Examples

We provide a set of **example scripts** (located in `examples/`) to demonstrate various use cases:

1. **`example_xv_selection.py`**  
   Demonstrates how to use `XVSelector` to pick a 15-player squad from a larger candidate pool.

2. **`example_xi_selection.py`**  
   Shows using `XISelector` to choose an 11-player lineup from a 15-player squad.

3. **`example_update_xv.py`**  
   Demonstrates how `UpdateXVSelector` can modify an existing 15-player squad, making a limited number of transfers.

4. **`example_custom_constraints.py`**  
   Illustrates creating **custom constraints** with `BaseSelector` for specialized optimization scenarios (e.g., ensuring at least one player from a specific team).

5. **`model/example_fpl_points_model.py`**  
   A simple script using the `FPLPointsModel` to fit and predict on a tiny dataset.

6. **`model/example_extend_base_model.py`**  
   Shows how to **extend** the base Bayesian model (`BaseBayesianModel`) to create a brand-new custom points model for your own use case—then fit and predict with it.

By following these examples, you can see how **Lionel**’s modular architecture supports everything from standard FPL squad selection to **completely new** modeling or selection strategies.

---

## Data Sources

- **Fantasy Premier League (Pre-2024/25):** [Vaastav/Fantasy-Premier-League](https://github.com/vaastav/Fantasy-Premier-League)  
- **Betting Odds:** [The Odds API](https://the-odds-api.com)

---

## Web Application

A simple **web application** built with Streamlit offers a GUI to explore model predictions and recommended teams:

[https://lionel.streamlit.app/](https://lionel.streamlit.app/)

---

## Contributing

We welcome contributions of all kinds—whether it’s new selection strategies, improved data pipelines, or expanded analytics.

### How to Contribute

1. **Fork the Repository**  
   Click the **Fork** button on the repository page.

2. **Clone Your Fork**
   ```bash
   git clone https://github.com/your-username/lionel.git
   cd lionel
   ```

3. **Create a New Branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

4. **Implement Your Feature**  
   Add or modify code in a clear, maintainable way.

5. **Commit Changes**
   ```bash
   git commit -m "Add feature: your description"
   ```

6. **Push to Your Fork**
   ```bash
   git push origin feature/your-feature-name
   ```

7. **Create a Pull Request**  
   Open a PR from your fork to the main repository.

### Code of Conduct

Please adhere to our [Code of Conduct](CODE_OF_CONDUCT.md) to foster an inclusive environment.

---

## License

This project is licensed under the [MIT License](LICENSE).

---

## Contact

- **Email:** [jtobyh@gmail.com](mailto:jtobyh@gmail.com)
- **GitHub Issues:** [Issues Page](https://github.com/jth500/lionel/issues)

---

## Acknowledgements


- **[PyMC](https://www.pymc.io/welcome.html)** for Bayesian modeling 
   - [Baio, Blangiardo (2010)](https://discovery.ucl.ac.uk/id/eprint/16040/) and [Alan Turing Institute](https://github.com/alan-turing-institute/AIrsenal) for model choice.
- **[PuLP](https://coin-or.github.io/pulp/)** for linear programming  
- **[Luigi](https://github.com/spotify/luigi)** for pipeline orchestration  
- **[Vaastav/Fantasy-Premier-League](https://github.com/vaastav/Fantasy-Premier-League)** for historical FPL data  
- **[The Odds API](https://the-odds-api.com)** for betting odds data  

