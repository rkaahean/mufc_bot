import pandas as pd
import datetime
import tweepy
import os
import schedule
import time
import pendulum

FBREF_MUFC_URL = "https://fbref.com/en/squads/19538871/all_comps/Manchester-United-Stats-All-Competitions"
FBREF_BASE_URL = "https://fbref.com"

WIN_EMOJI = "✅"
LOSS_EMOJI = "❌"
DRAW_EMOJI = "➖"
FORM_EMOJIS = {"W": WIN_EMOJI, "D": DRAW_EMOJI, "L": LOSS_EMOJI}
NOTIFICATION_EMOJI = "🔔"


TWEETS = {
    "pre_match": {
        "form": "@ManUtd's recent form\n {}",
        "upcoming": "🔔 NEXT FIXTURE\n{team_name} ({venue}) in the {competition}",
    }
}


def get_twitter_api():
    auth = tweepy.OAuth1UserHandler(
        access_token=os.environ.get("ACCESS_TOKEN"),
        access_token_secret=os.environ.get("ACCESS_TOKEN_SECRET"),
        consumer_key=os.environ.get("API_KEY"),
        consumer_secret=os.environ.get("API_SECRET_KEY"),
    )
    api = tweepy.API(auth)
    return api


def get_prematch_report():
    """
    Match report to publish before matchday
    """

    # get all the relevant links
    links = pd.read_html(FBREF_MUFC_URL, match="Head-to-Head", extract_links="all")
    fixtures = pd.read_html(FBREF_MUFC_URL, match="Head-to-Head")
    results = pd.read_html(FBREF_MUFC_URL, match="Match Report")

    # format the dataframes
    results = results[0]
    results = results[~results["GF"].isna()]
    fixtures = fixtures[0]
    links = links[0]
    links.columns = fixtures.columns

    # get next fixture date
    upcoming_fixtures = fixtures[fixtures[["Captain"]].isna().all(1)]
    next_fixture_date = upcoming_fixtures.head(1)["Date"].values[0]
    next_fixture_time = upcoming_fixtures.head(1)["Time"].values[0]
    # get the match report link for next fixture
    match_report_link = links[links["Date"] == (next_fixture_date, None)]
    match_report_link = match_report_link["Match Report"].values[0][1]
    h2hlink = FBREF_BASE_URL + match_report_link
    # get pandas df on head2head
    df_head2head = pd.read_html(h2hlink, match="Match Report")[0]
    df_head2head["Date"] = pd.to_datetime(df_head2head["Date"], errors="coerce")
    h2hfiltered = df_head2head[df_head2head["Date"] < datetime.datetime.now()].dropna(
        subset=["Score"]
    )
    h2hfiltered.head(5).apply(
        lambda x: print(
            "Comp: " + x.Comp + " " + x.Home + "\t" + x.Score + "\t" + x.Away
        ),
        axis=1,
    )

    return {
        "form": results["Result"].tail(5),
        "head2head": h2hfiltered.head(5),
        "fixture": upcoming_fixtures.head(1).squeeze().to_dict(),
        "next_fixture_date": pendulum.parse(
            next_fixture_date + " " + next_fixture_time, tz="Europe/London"
        ),
    }


def _get_match_emoji(primary_team, venue, score):
    scores = score.split("-")
    if score[0] == score[2]:
        return DRAW_EMOJI
    elif int(score[0]) > int(score[2]) and venue != primary_team:
        return LOSS_EMOJI
    else:
        return WIN_EMOJI


def publish_prematch_report():
    """
    Publish the prematch report data
    """
    running_env = os.environ.get("RUN_ENV")

    data = get_prematch_report()
    form, head2head, fixture, next_date = (
        data["form"],
        data["head2head"],
        data["fixture"],
        data["next_fixture_date"],
    )
    time_to_match = next_date.diff(pendulum.now(tz="Europe/London")).in_minutes()

    if (time_to_match >= 1500 or time_to_match < 1400) and running_env == "PROD":
        return

    form = data["form"].values
    formatted_form = ""
    for i in form:
        formatted_form += FORM_EMOJIS[i] + " "

    form_tweet = TWEETS.get("pre_match").get("form").format(formatted_form)
    upcoming_tweet = (
        TWEETS.get("pre_match")
        .get("upcoming")
        .format(
            venue=fixture["Venue"][0],
            team_name=fixture["Opponent"],
            competition=fixture["Comp"],
        )
    )
    final_tweet = upcoming_tweet + "\n\n" + form_tweet + "\n"

    h2htext = "PREVIOUS RESULTS\n\n"
    for _, x in head2head.iterrows():
        h2htext += (
            _get_match_emoji("Manchester Utd", x.Home, x.Score)
            + "  "
            + x.Home
            + "  "
            + x.Score
            + "  "
            + x.Away
            + "\n"
        )

    if running_env == "PROD":
        print("Publishing to twitter...")
        api = get_twitter_api()
        tweet = api.update_status(final_tweet)
        api.update_status(
            h2htext,
            in_reply_to_status_id=tweet.id_str,
            auto_populate_reply_metadata=True,
        )


running_env = os.environ.get("RUN_ENV")
if running_env == "PROD":
    schedule.every(100).minutes.do(publish_prematch_report)
    while True:
        schedule.run_pending()
        time.sleep(1)
else:
    publish_prematch_report()
