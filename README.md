# get-illinois-events

An AWS Lambda function to fetch events scheduled in classrooms at the University of Illinois Urbana-Champaign (UIUC).

UIUC's Facility Scheduling and Logistics team regularly posts updates to a [Daily Event Summary Tableau dashboard](https://operations.web.illinois.edu/events/). The code in [lambda_function.py](lambda_function.py) gets event data for recent and upcoming events (see [Data limitations](#data-limitations)), processes the data, and inserts the events into a MongoDB database.

## Data limitations

The [Daily Event Summary](https://operations.web.illinois.edu/events/) only contains events for today as well as events that occurred in the past 13 days or will occur within the next 14 days.

- Example: If today is May 3, event data is available from Apr 20 – May 17 (inclusive).

## Usage (local)

1. Clone this repository
2. Install Python 3.11
3. Install dependencies: `pip install -r requirements.txt`
4. Set up environment variable:
   - `MONGODB_URI`: URI for connecting to your MongoDB database
     - `export MONGODB_URI="mongodb+srv://[YOUR-DB-HERE]/?retryWrites=true&w=majority"`
5. Run `python lambda_function.py`

## Lambda setup

TODO

## License

This project is licensed under the GNU General Public License v3.0. See the [LICENSE](LICENSE) file for details.
