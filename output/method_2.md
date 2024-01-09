step_1:
gaps between specific outlet sales that is 6 months or less, i used interpolation to generate these months

step_2:
get available averages for outlet sales based on:

- brand
- location_type
- area_level_3 if available, then area_level_2
- sales_month
- sales_year
  and fill them
  apply step_1 until there is no gaps

by this point i got 110930 records, 393819 records to go
