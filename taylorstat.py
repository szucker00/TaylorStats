import pandas as pd
import numpy as np
import sys
import json

def ms2hr(ms_val):
    return ms_val/(1000*60*60)

def file2df(stream_file_list):
    dfs = []

    for f_name in stream_file_list:
        with open(f_name) as f:
            df_from_json = pd.json_normalize(json.loads(f.read()))
            dfs.append(df_from_json)

    df = pd.concat(dfs, sort=False)
    return df


def top_taylor(df, top=12):
    df_ts=df[df['artistName'] == 'Taylor Swift']
    conditions = [
        (df_ts['trackName'].str.contains("Tim McGraw")) | (df_ts['trackName'].str.contains("Picture To Burn")) \
        | (df_ts['trackName'].str.contains("Teardrops On My Guitar")) | (df_ts['trackName'].str.contains("A Place in this World")) \
        | (df_ts['trackName'].str.contains("Cold As You")) | (df_ts['trackName'].str.contains("The Outside")) \
        | (df_ts['trackName'].str.contains("Tied Together with a Smile")) | (df_ts['trackName'].str.contains("Stay Beautiful")) \
        | (df_ts['trackName'].str.contains("Said No")) | (df_ts['trackName'].str.contains("Mary")) \
        | (df_ts['trackName'].str.contains("Our Song")) | (df_ts['trackName'].str.contains("Only Me When")) \
        | (df_ts['trackName'].str.contains("Invisible")) | (df_ts['trackName'].str.contains("A Perfectly Good Heart")),

        (df_ts['trackName'].str.contains("Fearless")) | (df_ts['trackName'].str.contains("Fifteen")) \
        | (df_ts['trackName'].str.contains("Love Story")) | (df_ts['trackName'].str.contains("Hey Stephen")) \
        | (df_ts['trackName'].str.contains("White Horse")) | (df_ts['trackName'].str.contains("You Belong")) \
        | (df_ts['trackName'].str.contains("Breathe")) | (df_ts['trackName'].str.contains("Tell Me Why")) \
        | (df_ts['trackName'].str.contains("Not Sorry")) | (df_ts['trackName'].str.contains("The Way I Loved You")) \
        | (df_ts['trackName'].str.contains("Always")) | (df_ts['trackName'].str.contains("The Best Day")) \
        | (df_ts['trackName'].str.contains("Change")) | (df_ts['trackName'].str.contains("Jump Then Fall")) \
        | (df_ts['trackName'].str.contains("Untouchable")) | (df_ts['trackName'].str.contains("Come In With The Rain")) \
        | (df_ts['trackName'].str.contains("Superstar")) | (df_ts['trackName'].str.contains("The Other Side Of The Door")) \
        | (df_ts['trackName'].str.contains("Fairytale")) | (df_ts['trackName'].str.contains("You All Over Me")) \
        | (df_ts['trackName'].str.contains("Perfectly Fine")) | (df_ts['trackName'].str.contains("We Were Happy")) \
        | (df_ts['trackName'].str.contains("That’s When")) | (df_ts['trackName'].str.contains("Don’t You")) \
        | (df_ts['trackName'].str.contains("Bye Bye Baby")),

        (df_ts['trackName'].str.contains("Mine")) | (df_ts['trackName'].str.contains("Sparks Fly")) \
        | (df_ts['trackName'].str.contains("Back To December")) | (df_ts['trackName'].str.contains("Speak Now")) \
        | (df_ts['trackName'].str.contains("Dear John")) | (df_ts['trackName'].str.contains("Mean")) \
        | (df_ts['trackName'].str.contains("The Story Of Us")) | (df_ts['trackName'].str.contains("Never Grow Up")) \
        | (df_ts['trackName'].str.contains("Enchanted")) | (df_ts['trackName'].str.contains("Better Than Revenge")) \
        | (df_ts['trackName'].str.contains("Innocent")) | (df_ts['trackName'].str.contains("Haunted")) \
        | (df_ts['trackName'].str.contains("Last Kiss")) | (df_ts['trackName'].str.contains("Long Live")) \
        | (df_ts['trackName'].str.contains("Ours")) | (df_ts['trackName'].str.contains("If This Was A Movie")) \
        | (df_ts['trackName'].str.contains("Superman")) | (df_ts['trackName'].str.contains("Electric Touch")) \
        | (df_ts['trackName'].str.contains("Emma")) | (df_ts['trackName'].str.contains("I Can See You")) \
        | (df_ts['trackName'].str.contains("Castles Crumbling")) | (df_ts['trackName'].str.contains("Foolish One")) \
        | (df_ts['trackName'].str.contains("Timeless")),

        (df_ts['trackName'].str.contains("State Of Grace")) | (df_ts['trackName'].str.contains("Red")) \
        | (df_ts['trackName'].str.contains("Treacherous")) | (df_ts['trackName'].str.contains("I Knew You Were Trouble")) \
        | (df_ts['trackName'].str.contains("All Too Well")) | (df_ts['trackName'].str.contains("22")) \
        | (df_ts['trackName'].str.contains("I Almost Do")) | (df_ts['trackName'].str.contains("We Are Never Ever")) \
        | (df_ts['trackName'].str.contains("Stay Stay Stay")) | (df_ts['trackName'].str.contains("The Last Time")) \
        | (df_ts['trackName'].str.contains("Holy Ground")) | (df_ts['trackName'].str.contains("Sad Beautiful Tragic")) \
        | (df_ts['trackName'].str.contains("The Lucky One")) | (df_ts['trackName'].str.contains("Everything Has Changed")) \
        | (df_ts['trackName'].str.contains("Starlight")) | (df_ts['trackName'].str.contains("Begin Again")) \
        | (df_ts['trackName'].str.contains("The Moment I Knew")) | (df_ts['trackName'].str.contains("Come Back")) \
        | (df_ts['trackName'].str.contains("Girl At Home")) | (df_ts['trackName'].str.contains("Ronan")) \
        | (df_ts['trackName'].str.contains("Better Man")) | (df_ts['trackName'].str.contains("Nothing New")) \
        | (df_ts['trackName'].str.contains("Babe")) | (df_ts['trackName'].str.contains("Message In A Bottle")) \
        | (df_ts['trackName'].str.contains("I Bet You Think About Me")) | (df_ts['trackName'].str.contains("Forever Winter")) \
        | (df_ts['trackName'].str.contains("Run")) | (df_ts['trackName'].str.contains("The Very First Night")),

        (df_ts['trackName'].str.contains("Welcome To New York")) | (df_ts['trackName'].str.contains("Blank Space")) \
        | (df_ts['trackName'].str.contains("Style")) | (df_ts['trackName'].str.contains("Out Of The Woods")) \
        | (df_ts['trackName'].str.contains("All You Had To Do Was Stay")) | (df_ts['trackName'].str.contains("Shake It Off")) \
        | (df_ts['trackName'].str.contains("I Wish You Would")) | (df_ts['trackName'].str.contains("Bad Blood")) \
        | (df_ts['trackName'].str.contains("Wildest Dreams")) | (df_ts['trackName'].str.contains("How You Get The Girl")) \
        | (df_ts['trackName'].str.contains("This Love")) | (df_ts['trackName'].str.contains("I Know Places")) \
        | (df_ts['trackName'].str.contains("Clean")) | (df_ts['trackName'].str.contains("Wonderland")) \
        | (df_ts['trackName'].str.contains("You Are In Love")) | (df_ts['trackName'].str.contains("New Romantics")) \
        | (df_ts['trackName'].str.contains("Slut!")) | (df_ts['trackName'].str.contains("Say")) \
        | (df_ts['trackName'].str.contains("Now That We")) | (df_ts['trackName'].str.contains("Suburban Legends")) \
        | (df_ts['trackName'].str.contains("Is It Over")),

        (df_ts['trackName'].str.contains("Ready For")) | (df_ts['trackName'].str.contains("End Game")) \
        | (df_ts['trackName'].str.contains("I Did Something Bad")) | (df_ts['trackName'].str.contains("Blame Me")) \
        | (df_ts['trackName'].str.contains("Delicate")) | (df_ts['trackName'].str.contains("Look What You Made Me Do")) \
        | (df_ts['trackName'].str.contains("So It Goes")) | (df_ts['trackName'].str.contains("Gorgeous")) \
        | (df_ts['trackName'].str.contains("Getaway Car")) | (df_ts['trackName'].str.contains("King Of My Heart")) \
        | (df_ts['trackName'].str.contains("Dancing With Our Hands Tied")) | (df_ts['trackName'].str.contains("Dress")) \
        | (df_ts['trackName'].str.contains("This Is Why We")) | (df_ts['trackName'].str.contains("Call It What You Want")) \
        | (df_ts['trackName'].str.contains("New Year")),

        (df_ts['trackName'].str.contains("I Forgot That You Existed")) | (df_ts['trackName'].str.contains("Cruel Summer")) \
        | (df_ts['trackName'].str.contains("Lover")) | (df_ts['trackName'].str.contains("The Man")) \
        | (df_ts['trackName'].str.contains("The Archer")) | (df_ts['trackName'].str.contains("I Think He Knows")) \
        | (df_ts['trackName'].str.contains("Miss Americana")) | (df_ts['trackName'].str.contains("Paper Rings")) \
        | (df_ts['trackName'].str.contains("Cornelia Street")) | (df_ts['trackName'].str.contains("Death By A Thousand Cuts")) \
        | (df_ts['trackName'].str.contains("London Boy")) | (df_ts['trackName'].str.contains("Get Better")) \
        | (df_ts['trackName'].str.contains("False God")) | (df_ts['trackName'].str.contains("You Need To Calm Down")) \
        | (df_ts['trackName'].str.contains("Afterglow")) | (df_ts['trackName'].str.contains("ME!")) \
        | (df_ts['trackName'].str.contains("Nice To Have A Friend")) | (df_ts['trackName'].str.contains("Daylight")) \
        | (df_ts['trackName'].str.contains("All Of The Girls You Loved Before")),

        (df_ts['trackName'].str.contains("the 1")) | (df_ts['trackName'].str.contains("cardigan")) \
        | (df_ts['trackName'].str.contains("exile")) | (df_ts['trackName'].str.contains("the last great american dynasty")) \
        | (df_ts['trackName'].str.contains("my tears ricochet")) | (df_ts['trackName'].str.contains("mirrorball")) \
        | (df_ts['trackName'].str.contains("seven")) | (df_ts['trackName'].str.contains("august")) \
        | (df_ts['trackName'].str.contains("this is me trying")) | (df_ts['trackName'].str.contains("illicit affairs")) \
        | (df_ts['trackName'].str.contains("invisible string")) | (df_ts['trackName'].str.contains("mad woman")) \
        | (df_ts['trackName'].str.contains("epiphany")) | (df_ts['trackName'].str.contains("betty")) \
        | (df_ts['trackName'].str.contains("peace")) | (df_ts['trackName'].str.contains("hoax")) \
        | (df_ts['trackName'].str.contains("the lakes")),

        (df_ts['trackName'].str.contains("willow")) | (df_ts['trackName'].str.contains("champagne problems")) \
        | (df_ts['trackName'].str.contains("gold rush")) | (df_ts['trackName'].str.contains("the damn season")) \
        | (df_ts['trackName'].str.contains("tolerate it")) | (df_ts['trackName'].str.contains("no crime")) \
        | (df_ts['trackName'].str.contains("happiness")) | (df_ts['trackName'].str.contains("dorothea")) \
        | (df_ts['trackName'].str.contains("coney island")) | (df_ts['trackName'].str.contains("ivy")) \
        | (df_ts['trackName'].str.contains("cowboy like me")) | (df_ts['trackName'].str.contains("long story short")) \
        | (df_ts['trackName'].str.contains("marjorie")) | (df_ts['trackName'].str.contains("closure")) \
        | (df_ts['trackName'].str.contains("evermore")) | (df_ts['trackName'].str.contains("right where you left me")) \
        | (df_ts['trackName'].str.contains("time to go")),

        (df_ts['trackName'].str.contains("Lavender Haze")) | (df_ts['trackName'].str.contains("Maroon")) \
        | (df_ts['trackName'].str.contains("Anti-Hero")) | (df_ts['trackName'].str.contains("Snow On The Beach")) \
        | (df_ts['trackName'].str.contains("On Your Own")) | (df_ts['trackName'].str.contains("Midnight Rain")) \
        | (df_ts['trackName'].str.contains("Question")) | (df_ts['trackName'].str.contains("Vigilante Shit")) \
        | (df_ts['trackName'].str.contains("Bejeweled")) | (df_ts['trackName'].str.contains("Labyrinth")) \
        | (df_ts['trackName'].str.contains("Karma")) | (df_ts['trackName'].str.contains("Sweet Nothing")) \
        | (df_ts['trackName'].str.contains("Mastermind")) | (df_ts['trackName'].str.contains("The Great War")) \
        | (df_ts['trackName'].str.contains("Bigger Than The Whole Sky")) | (df_ts['trackName'].str.contains("Paris")) \
        | (df_ts['trackName'].str.contains("High Infidelity")) | (df_ts['trackName'].str.contains("Glitch")) \
        | (df_ts['trackName'].str.contains("Would've")) | (df_ts['trackName'].str.contains("Dear Reader")) \
        | (df_ts['trackName'].str.contains("Hits Different")) | (df_ts['trackName'].str.contains("Losing Me")),

        (df_ts['trackName'].str.contains("Fortnight")) | (df_ts['trackName'].str.contains("The Tortured Poets")) \
        | (df_ts['trackName'].str.contains("My Boy Only Breaks")) | (df_ts['trackName'].str.contains("Down Bad")) \
        | (df_ts['trackName'].str.contains("So Long")) | (df_ts['trackName'].str.contains("But Daddy I Love Him")) \
        | (df_ts['trackName'].str.contains("Fresh Out The Slammer")) | (df_ts['trackName'].str.contains("Florida!!!")) \
        | (df_ts['trackName'].str.contains("Guilty as")) | (df_ts['trackName'].str.contains("Afraid of Little")) \
        | (df_ts['trackName'].str.contains("I Can Fix Him")) | (df_ts['trackName'].str.contains("loml")) \
        | (df_ts['trackName'].str.contains("I Can Do It With")) | (df_ts['trackName'].str.contains("The Smallest Man")) \
        | (df_ts['trackName'].str.contains("The Alchemy")) | (df_ts['trackName'].str.contains("Clara Bow")) \
        | (df_ts['trackName'].str.contains("The Black Dog")) | (df_ts['trackName'].str.contains("imgonnagetyouback")) \
        | (df_ts['trackName'].str.contains("The Albatross")) | (df_ts['trackName'].str.contains("Chloe or Sam")) \
        | (df_ts['trackName'].str.contains("How Did It")) | (df_ts['trackName'].str.contains("So High School")) \
        | (df_ts['trackName'].str.contains("I Hate It Here")) | (df_ts['trackName'].str.contains("thanK you aIMee")) \
        | (df_ts['trackName'].str.contains("I Look in People's Windows")) | (df_ts['trackName'].str.contains("The Prophecy")) \
        | (df_ts['trackName'].str.contains("Cassandra")) | (df_ts['trackName'].str.contains("Peter")) \
        | (df_ts['trackName'].str.contains("The Bolter")) | (df_ts['trackName'].str.contains("Robin")) \
        | (df_ts['trackName'].str.contains("The Manuscript"))
        ]
    choices = ["Taylor Swift","Fearless", "Speak Now", "Red", "1989", "reputation", "Lover", "folklore", "evermore", "Midnights", "THE TORTURED POETS DEPARTMENT"]
    df_ts['albumName'] = np.select(conditions,choices, default="Other")

    df_top = df_ts.groupby(['albumName'], as_index=False) \
        .agg({'endTime':'count', 'msPlayed':'sum'}) \
        .rename(columns={'endTime':'noStreams', 'msPlayed':'streamTimeMs'})
    df_top['streamTimeHr'] = ms2hr(df_top['streamTimeMs'])
    df_top = df_top.sort_values(by=['noStreams'], ascending=False)
    df_top = df_top.head(top)

    print(df_top)
    return df_top


def main(stream_file_list):
    df = file2df(stream_file_list)
    top_taylor(df)


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        main(sys.argv[1:])