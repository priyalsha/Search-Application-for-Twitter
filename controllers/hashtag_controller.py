from app import app
@app.route("/user/hashtags")
def gethashTags():
    return "hashtags Page"
