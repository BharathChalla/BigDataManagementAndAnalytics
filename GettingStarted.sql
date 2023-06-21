// Use Connection Type: MongoDB Atlas SRV Protocol
// https://www.jetbrains.com/help/datagrip/mongodb.html
// https://www.jetbrains.com/pycharm/guide/tutorials/intro-aws/atlas/
use sample_airbnb;
db.listingsAndReviews.find({})
db.listingsAndReviews.countDocuments()

db.listingsAndReviews.find({
    "address.country": "Canada",
    bedrooms: 2
})

db.listingsAndReviews.find({
    "address.country": "Canada",
    bedrooms: 2,
    amenities: mb.regex.contains("kitchen")
})