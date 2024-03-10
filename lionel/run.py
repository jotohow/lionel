from scrape.players.process import FPLProcessor
from scrape.bet.scrape import FutureBetScraper
from team.team import Team
from scrape.combine import BetFPLCombiner


def run(season, next_gameweek):

    # Update FPL stats
    fpl_processor = FPLProcessor(season, next_gameweek)
    player_stats = fpl_processor.player_stats

    # Update betting
    bet_scraper = FutureBetScraper()
    bet_scraper.run_scrape()
    win_odds = bet_scraper.to_df()

    # Combine FPL and betting odds
    df_next_game = BetFPLCombiner(
        next_gameweek, win_odds, player_stats
    ).prepare_next_gw()

    # Update team choices
    team = Team(season, next_gameweek, df_next_game)
    team.pick_xi()
    return team
