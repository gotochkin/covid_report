Ontario Covid19
Copyright © 2021 Gleb Otochkin

blog: https://blog.gleb.ca

Twitter: @sky_vst

Created using data from the "Open Government" Ontario datasets: https://www.ontario.ca/page/open-government
Data are used under Open Government License - Ontario: https://www.ontario.ca/page/open-government-licence-ontario

The loadtodwCovid.py file is getting data from https://data.ontario.ca and merge it to the Oracle ATP 
Oracle Apex is used as the fronend and represent the data as charts and tables.
The data are presented "as is" and cannot be used for any official refernces. 
The rate of hospitalization calculated using population projection and on data from vaccination by at least two doses.

The loadtodwCovid.py requires Oracle instant client libraries, correct TNS_ADMIN defined location and four arguments:
-dbn THS name for the connection. Usually you can find it in tnsnames.ora after you unpack the wallet zip file
-dbu the Oracle schema(user) in the ATP
-dbp the password for the Oracle user

Example:
python3 loadtodwCovid.py -dbn atp01_tp -dbu covid -dbp Password01 -ldir /u01/app/oracle/instantclient_19_8
