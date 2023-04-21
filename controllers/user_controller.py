from app import app
@app.route("/user/tweet")
def getUserTweets():
    return "User Tweets"    