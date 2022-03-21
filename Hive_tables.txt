create table review1
(
review_id string,
user_id string,
business_id string,
stars int,
useful int,
funny int,
cool int,
text string,
`date` string
)
row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/user/saif/HFS/output/project/review';


create table business
(
address string,
attributes struct<AcceptsInsurance:string,AgesAllowed: string,Alcohol: string,Ambience: string,
BYOB: string,BYOBCorkage: string,BestNights: string,BikeParking: string,BusinessAcceptsBitcoin: string,
BusinessAcceptsCreditCards: string,BusinessParking: string,ByAppointmentOnly: string,Caters: string,CoatCheck: string,
Corkage: string,DietaryRestrictions: string,DogsAllowed: string,DriveThru: string,GoodForDancing: string,GoodForKids: string,
GoodForMeal: string,HairSpecializesIn: string,HappyHour: string,HasTV: string,Music: string ,NoiseLevel: string,
Open24Hours: string,OutdoorSeating: string,RestaurantsAttire: string,RestaurantsCounterService: string,RestaurantsDelivery: string,
RestaurantsGoodForGroups: string,RestaurantsPriceRange2: string,RestaurantsReservations: string,RestaurantsTableService: string,
RestaurantsTakeOut: string,Smoking: string,WheelchairAccessible: string,WiFi: string>,
business_id string,
categories string,
city string,
hours struct<Friday: string,Monday: string,Saturday: string,Sunday: string,Thursday: string,Tuesday: string,Wednesday: string>,
is_open string,
latitude double,
longitude double,
name string,
postal_code string,
review_count string,
stars double,
state string,
Review_status string
)
row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/user/saif/HFS/output/project/business';
