# Visor
Visor is a Sentries developed tool for generating data on NFT markets as well as scraping Discord and Twitter statistics for use in statistical analysis. It currently fetches data from Magic Eden for project information and details as well as market statistics like trading activity.

# Installation
This repository requires node, yarn, and postgres to operate.

You can run model.sql to generate the data model for the system to run.


`git clone https://github.com/sentrieshq/visor.git`

`cd visor`

`yarn install`

`cp example.env .env`

`vim .env`

Update with all your Potsgres details.

This system is designed to operate with pm2 installed.

# Operation

`LAUNCHPAD=true yarn populate`

`DROPS=true yarn populate`

`MAGICEDEN=true yarn populate`

`PRICE=true yarn populate`

`DISCORD=true yarn populate`

`TWITTER=true yarn populate`

`ACTIVITY=true yarn populate`

# ToDo
Update instructions to include Postgres details.

Update instructions to include functional diagrams.

Expand documentation.