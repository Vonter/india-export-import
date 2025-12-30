# india-export-import

The source data is fetched from the [Foreign Trade Data Dissemination Portal](https://ftddp.dgciskol.gov.in/dgcis/principalcommditysearch.html)

## Data dictionary

Each row in the dataset represents a trade entry for a single commodity, country, port, year, month, and type (import or export).

| Variable | Type | Description |
|----------|------|-------------|
| Commodity | string | Name of the commodity |
| Country | string | Name of the foreign country |
| Port | string | Name of the port in India |
| Year | int32 | Year for the import/export activity |
| Month | int32 | Month for the import/export activity |
| Type | category | Type of trade (Import or Export) |
| Quantity | int64 | Quantity of the commodity |
| Unit | string | Unit for the quantity |
| INR Value | int64 | Value of the commodity in INR |
| USD Value | int64 | Value of the commodity in USD |
