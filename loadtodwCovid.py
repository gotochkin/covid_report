#!/usr/bin/env python3
###############################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# loadtodwCovid.py
#
# @author: Gleb Otochkin
#
# Supports Python 3 and above
#
###############################################################################
import gzip
import csv
import os
import oci
import datetime
import cx_Oracle
import argparse
import certifi
import urllib
#import urllib.request
#import urllib.error
#
###############################################################################
# Defaults
###############################################################################
work_dir = os.curdir + '/work_dir'
###############################################################################
# Command line arguments
###############################################################################
def set_arguments():
    args = argparse.ArgumentParser()

    args.add_argument('-c',type=argparse.FileType('r'),dest='config',help='Config file')
    args.add_argument('-dbn', default="", dest='dbname', help='ADB Name')
    args.add_argument('-dbu', default="", dest='dbuser', help='ADB User')
    args.add_argument('-dbp', default="", dest='dbpass', help='ADB Password')
    args.add_argument('-ldir', default="", dest='ldir', help='OCI client libraries')

    res = args.parse_args()
    return res
###############################################################################
# Load config
###############################################################################
# Load vaccine doses distributed ontario
###############################################################################
def load_vaccine_doses(connection, vaccine_doses_url):
    file_num = 0
    row_num = 0

    try:
        f = vaccine_doses_url
        fname = f.rsplit('/',1)[-1]
        print("   Loading file " + fname)
        file_id = fname[:-7]
        print(file_id)
        #request = urllib.request.urlopen(vaccine_doses_url)
        with urllib.request.urlopen(vaccine_doses_url) as f_in:
            csv_read = csv.DictReader(f_in.read().decode('utf-8').splitlines())
            # Load batch size for cx_oracle
            batch_size = 2000
            array_size = 500
            # Construct SQL
            sqlstr = "INSERT INTO VACCINE_DOSES_ONT_NEW ("
            sqlstr += "REPORT_DATE,"
            sqlstr += "PREVIOUS_DAY_TOTAL_DOSES_ADMINISTERED,"
            sqlstr += "PREVIOUS_DAY_AT_LEAST_ONE,"
            sqlstr += "PREVIOUS_DAY_FULLY_VACCINATED,"
            sqlstr += "TOTAL_DOSES_ADMINISTERED,"
            sqlstr += "TOTAL_INDIVIDUALS_AT_LEAST_ONE,"
            sqlstr += "TOTAL_INDIVIDUALS_PARTIALLY_VACCINATED,"
            sqlstr += "TOTAL_DOSES_IN_FULLY_VACCINATED_INDIVIDUALS,"
            sqlstr += "TOTAL_INDIVIDUALS_FULLY_VACCINATED,"
            sqlstr += "TOTAL_INDIVIDUALS_3DOSES"
            sqlstr += ") VALUES ("
            sqlstr += "to_date(:1,'YYYY-MM-DD'), :2, :3, :4, :5, :6, :7, :8, :9, :10"
            sqlstr += ") "
            
            # Open cursor
            #cursor = cx_Oracle.Cursor(connection)
            cursor = connection.cursor()
            #truncate table VACCINE_DOSES_ONT_NEW
            sqltrun = "truncate table VACCINE_DOSES_ONT_NEW"
            cursor.execute(sqltrun)
            # Set array size
            cursor.setinputsizes(None, array_size)

            ldata = []
            for line in csv_read:
                #load columns to variables 
                report_date = read_column_value('report_date', line)
                previous_day_total_doses_administered = read_column_value('previous_day_total_doses_administered',line)
                previous_day_at_least_one = read_column_value('previous_day_at_least_one',line)
                previous_day_fully_vaccinated = read_column_value('previous_day_fully_vaccinated',line)
                total_doses_administered = read_column_value('total_doses_administered',line)
                total_individuals_at_least_one = read_column_value('total_individuals_at_least_one',line)
                total_individuals_partially_vaccinated = read_column_value('total_individuals_partially_vaccinated',line)
                total_doses_in_fully_vaccinated_individuals = read_column_value('total_doses_in_fully_vaccinated_individuals',line)
                total_individuals_fully_vaccinated = read_column_value('total_individuals_fully_vaccinated',line)
                total_individuals_3doses = read_column_value('total_individuals_3doses',line)
                #print(report_date + " " + previous_day_total_doses_administered)
                # create array
                line_data = (
                    report_date,
                    previous_day_total_doses_administered,
                    previous_day_at_least_one,
                    previous_day_fully_vaccinated,
                    total_doses_administered,
                    total_individuals_at_least_one,
                    total_individuals_partially_vaccinated,
                    total_doses_in_fully_vaccinated_individuals,
                    total_individuals_fully_vaccinated,
                    total_individuals_3doses
                )
                ldata.append(line_data)
                row_num += 1

                if len(ldata) % batch_size == 0:
                    cursor.executemany(sqlstr, ldata)
                    ldata = []

            if ldata:
                cursor.executemany(sqlstr, ldata)
            connection.commit()
            sqlstr = "merge "
            sqlstr += "into "
            sqlstr += "vaccine_doses_ont o "
            sqlstr += "using vaccine_doses_ont_new n "
            sqlstr += "on (n.REPORT_DATE=o.REPORT_DATE) "
            sqlstr += "when matched then "
            sqlstr += "update set "
            sqlstr += "o.PREVIOUS_DAY_TOTAL_DOSES_ADMINISTERED=n.PREVIOUS_DAY_TOTAL_DOSES_ADMINISTERED,"
            sqlstr += "o.PREVIOUS_DAY_AT_LEAST_ONE=n.PREVIOUS_DAY_AT_LEAST_ONE,"
            sqlstr += "o.PREVIOUS_DAY_FULLY_VACCINATED=n.PREVIOUS_DAY_FULLY_VACCINATED,"
            sqlstr += "o.TOTAL_DOSES_ADMINISTERED=n.TOTAL_DOSES_ADMINISTERED,"
            sqlstr += "o.TOTAL_INDIVIDUALS_AT_LEAST_ONE=n.TOTAL_INDIVIDUALS_AT_LEAST_ONE,"
            sqlstr += "o.TOTAL_INDIVIDUALS_PARTIALLY_VACCINATED=n.TOTAL_INDIVIDUALS_PARTIALLY_VACCINATED,"
            sqlstr += "o.TOTAL_DOSES_IN_FULLY_VACCINATED_INDIVIDUALS=n.TOTAL_DOSES_IN_FULLY_VACCINATED_INDIVIDUALS,"
            sqlstr += "o.TOTAL_INDIVIDUALS_FULLY_VACCINATED=n.TOTAL_INDIVIDUALS_FULLY_VACCINATED,"
            sqlstr += "o.TOTAL_INDIVIDUALS_3DOSES=n.TOTAL_INDIVIDUALS_3DOSES "
            sqlstr += "when not matched then insert "
            sqlstr += "(REPORT_DATE,"
            sqlstr += "PREVIOUS_DAY_TOTAL_DOSES_ADMINISTERED,"
            sqlstr += "PREVIOUS_DAY_AT_LEAST_ONE,"
            sqlstr += "PREVIOUS_DAY_FULLY_VACCINATED,"
            sqlstr += "TOTAL_DOSES_ADMINISTERED,"
            sqlstr += "TOTAL_INDIVIDUALS_AT_LEAST_ONE,"
            sqlstr += "TOTAL_INDIVIDUALS_PARTIALLY_VACCINATED,"
            sqlstr += "TOTAL_DOSES_IN_FULLY_VACCINATED_INDIVIDUALS,"
            sqlstr += "TOTAL_INDIVIDUALS_FULLY_VACCINATED,"
            sqlstr += "TOTAL_INDIVIDUALS_3DOSES) "
            sqlstr += "values "
            sqlstr += "(n.REPORT_DATE,"
            sqlstr += "n.PREVIOUS_DAY_TOTAL_DOSES_ADMINISTERED,"
            sqlstr += "n.PREVIOUS_DAY_AT_LEAST_ONE,"
            sqlstr += "n.PREVIOUS_DAY_FULLY_VACCINATED,"
            sqlstr += "n.TOTAL_DOSES_ADMINISTERED,"
            sqlstr += "n.TOTAL_INDIVIDUALS_AT_LEAST_ONE,"
            sqlstr += "n.TOTAL_INDIVIDUALS_PARTIALLY_VACCINATED,"
            sqlstr += "n.TOTAL_DOSES_IN_FULLY_VACCINATED_INDIVIDUALS,"
            sqlstr += "n.TOTAL_INDIVIDUALS_FULLY_VACCINATED,"
            sqlstr += "n.TOTAL_INDIVIDUALS_3DOSES)"
            #print(sqlstr)
            cursor.execute(sqlstr)
            connection.commit()

            cursor.close()
            print("   Downloaded and merged report " + fname + " - " + str(row_num) + " Rows Inserted")

    except Exception as er:
        print("\nError raised - " + str(er) + "\n")
###############################################################################
# Load Cases by vaccination status Ontario
###############################################################################
def load_cases_by_vacc_status(connection, cases_by_vacc_status_ont_url):
    file_num = 0
    row_num = 0

    try:
        f = cases_by_vacc_status_ont_url
        fname = f.rsplit('/',1)[-1]
        print("   Loading file " + fname)
        file_id = fname[:-7]
        print(file_id)
        
        with urllib.request.urlopen(cases_by_vacc_status_ont_url) as f_in:
            csv_read = csv.DictReader(f_in.read().decode('ascii').splitlines())
            # Load batch size for cx_oracle
            batch_size = 2000
            array_size = 500
            # Construct SQL
            sqlstr = "INSERT INTO CASES_BY_VACC_STATUS_ONT_NEW ("
            sqlstr += "DATE_U,"
            sqlstr += "COVID19_CASES_UNVAC,"
            sqlstr += "COVID19_CASES_PARTIAL_VAC,"
            sqlstr += "COVID19_CASES_FULL_VAC,"
            sqlstr += "COVID19_CASES_VAC_UNKNOWN,"
            sqlstr += "CASES_UNVAC_RATE_PER100K,"
            sqlstr += "CASES_PARTIAL_VAC_RATE_PER100K,"
            sqlstr += "CASES_FULL_VAC_RATE_PER100K,"
            sqlstr += "CASES_UNVAC_RATE_7MA,"
            sqlstr += "CASES_PARTIAL_VAC_RATE_7MA,"
            sqlstr += "CASES_FULL_VAC_RATE_7MA"
            sqlstr += ") VALUES ("
            sqlstr += "to_date(:1,'YYYY-MM-DD'), :2, :3, :4, :5, :6, :7, :8, :9, :10, :11"
            sqlstr += ") "
            
            # Open cursor
            #cursor = cx_Oracle.Cursor(connection)
            cursor = connection.cursor()
            #truncate table VACCINE_DOSES_ONT_NEW
            sqltrun = "truncate table CASES_BY_VACC_STATUS_ONT_NEW"
            cursor.execute(sqltrun)
            # Set array size
            cursor.setinputsizes(None, array_size)

            ldata = []
            for line in csv_read:
                #load columns to variables 
                date_u = read_column_value('Date', line)
                covid19_cases_unvac = read_column_value('covid19_cases_unvac',line)
                covid19_cases_partial_vac = read_column_value('covid19_cases_partial_vac',line)
                covid19_cases_full_vac = read_column_value('covid19_cases_full_vac',line)
                covid19_cases_vac_unknown = read_column_value('covid19_cases_vac_unknown',line)
                cases_unvac_rate_per100k = read_column_value('cases_unvac_rate_per100K',line)
                cases_partial_vac_rate_per100k = read_column_value('cases_partial_vac_rate_per100K',line)
                cases_full_vac_rate_per100k = read_column_value('cases_full_vac_rate_per100K',line)
                cases_unvac_rate_7ma = read_column_value('cases_unvac_rate_7ma',line)
                cases_partial_vac_rate_7ma = read_column_value('cases_partial_vac_rate_7ma',line)
                cases_full_vac_rate_7ma = read_column_value('cases_full_vac_rate_7ma',line)
                print(date_u + " " + cases_partial_vac_rate_per100k)
                # create array
                line_data = (
                    date_u,
                    covid19_cases_unvac,
                    covid19_cases_partial_vac,
                    covid19_cases_full_vac,
                    covid19_cases_vac_unknown,
                    cases_unvac_rate_per100k,
                    cases_partial_vac_rate_per100k,
                    cases_full_vac_rate_per100k,
                    cases_unvac_rate_7ma,
                    cases_partial_vac_rate_7ma,
                    cases_full_vac_rate_7ma
                )
                ldata.append(line_data)
                row_num += 1

                if len(ldata) % batch_size == 0:
                    cursor.executemany(sqlstr, ldata)
                    ldata = []

            if ldata:
                cursor.executemany(sqlstr, ldata)
            connection.commit()
            sqlstr = "merge "
            sqlstr += "into "
            sqlstr += "cases_by_vacc_status_ont o "
            sqlstr += "using cases_by_vacc_status_ont_new n "
            sqlstr += "on (n.DATE_U=o.DATE_U) "
            sqlstr += "when matched then "
            sqlstr += "update set "
            sqlstr += "o.COVID19_CASES_UNVAC=n.COVID19_CASES_UNVAC,"
            sqlstr += "o.COVID19_CASES_PARTIAL_VAC=n.COVID19_CASES_PARTIAL_VAC,"
            sqlstr += "o.COVID19_CASES_FULL_VAC=n.COVID19_CASES_FULL_VAC,"
            sqlstr += "o.COVID19_CASES_VAC_UNKNOWN=n.COVID19_CASES_VAC_UNKNOWN,"
            sqlstr += "o.CASES_UNVAC_RATE_PER100K=n.CASES_UNVAC_RATE_PER100K,"
            sqlstr += "o.CASES_PARTIAL_VAC_RATE_PER100K=n.CASES_PARTIAL_VAC_RATE_PER100K,"
            sqlstr += "o.CASES_FULL_VAC_RATE_PER100K=n.CASES_FULL_VAC_RATE_PER100K,"
            sqlstr += "o.CASES_UNVAC_RATE_7MA=n.CASES_UNVAC_RATE_7MA,"
            sqlstr += "o.CASES_PARTIAL_VAC_RATE_7MA=n.CASES_PARTIAL_VAC_RATE_7MA,"
            sqlstr += "o.CASES_FULL_VAC_RATE_7MA=n.CASES_FULL_VAC_RATE_7MA "
            sqlstr += "when not matched then insert "
            sqlstr += "(DATE_U,"
            sqlstr += "COVID19_CASES_UNVAC,"
            sqlstr += "COVID19_CASES_PARTIAL_VAC,"
            sqlstr += "COVID19_CASES_FULL_VAC,"
            sqlstr += "COVID19_CASES_VAC_UNKNOWN,"
            sqlstr += "CASES_UNVAC_RATE_PER100K,"
            sqlstr += "CASES_PARTIAL_VAC_RATE_PER100K,"
            sqlstr += "CASES_FULL_VAC_RATE_PER100K,"
            sqlstr += "CASES_UNVAC_RATE_7MA,"
            sqlstr += "CASES_PARTIAL_VAC_RATE_7MA,"
            sqlstr += "CASES_FULL_VAC_RATE_7MA) "
            sqlstr += "values "
            sqlstr += "(n.DATE_U,"
            sqlstr += "n.COVID19_CASES_UNVAC,"
            sqlstr += "n.COVID19_CASES_PARTIAL_VAC,"
            sqlstr += "n.COVID19_CASES_FULL_VAC,"
            sqlstr += "n.COVID19_CASES_VAC_UNKNOWN,"
            sqlstr += "n.CASES_UNVAC_RATE_PER100K,"
            sqlstr += "n.CASES_PARTIAL_VAC_RATE_PER100K,"
            sqlstr += "n.CASES_FULL_VAC_RATE_PER100K,"
            sqlstr += "n.CASES_UNVAC_RATE_7MA,"
            sqlstr += "n.CASES_PARTIAL_VAC_RATE_7MA,"
            sqlstr += "n.CASES_FULL_VAC_RATE_7MA)"
            #print(sqlstr)
            cursor.execute(sqlstr)
            connection.commit()

            cursor.close()
            print("   Downloaded and merged report " + fname + " - " + str(row_num) + " Rows Inserted")

    except Exception as er:
        print("\nError raised - " + str(er) + "\n")
###############################################################################
# Read columns values
###############################################################################
def read_column_value(col, arr):
    if col in arr:
        return arr[col]
    else:
        return ""

###############################################################################
# Load cost reports
###############################################################################
if not os.path.exists(work_dir):
    os.mkdir(work_dir)

###############################################################################
# print header 
###############################################################################
def print_header(name, w):
    opts = {0: 20, 1: 40, 2: 80}
    pr = int(opts[w])
    print("")
    print('#' * pr)
    print("#" + name.center(pr - 2, " ") + "#")
    print('#' * pr)

###############################################################################
# Main
###############################################################################
def main_process():
###############################################################################
# Load report
###############################################################################
    cli = set_arguments()
    if cli is None:
        exit()
    #config, signer = create_signer(cmd)
    try:
        print_header("COVID data to repostory",2)
        print("Starting at " + str(datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")))
        conf_file = oci.config.DEFAULT_LOCATION
        print(conf_file)
        print("\nStarting loading files ...")
        #load_cost_file_gz("/Users/otochkin/Downloads/reports_cost-csv_0001000000190535.csv.gz")
        print(certifi.where())
        vaccine_doses_url='https://data.ontario.ca/dataset/752ce2b7-c15a-4965-a3dc-397bf405e7cc/resource/8a89caa9-511c-4568-af89-7f2174b4378c/download/vaccine_doses.csv'
        cases_by_vacc_status_ont_url='https://data.ontario.ca/dataset/752ce2b7-c15a-4965-a3dc-397bf405e7cc/resource/eed63cf2-83dd-4598-b337-b288c0a89a16/download/cases_by_vac_status.csv'
        #try:
        #    urllib.request.urlretrieve(vaccine_doses_url, 'work_dir/vaccine_doses.csv')
        #except urllib.error.HTTPError as ex:
        #    print('Problem:', ex)
        #load_vaccine_doses(connection,'work_dir/vaccine_doses.csv')
    except Exception as er:
        print("\nError raised - " + str(er) + "\n")
    connection = None
    try:
        print("\nConnecting to database " + cli.dbname)
        cx_Oracle.init_oracle_client(lib_dir=cli.ldir)
        connection = cx_Oracle.connect(dsn=cli.dbname, user=cli.dbuser, password=cli.dbpass, encoding="UTF-8", nencoding="UTF-8")
        cursor = connection.cursor()
        print("   Connected")
    except cx_Oracle.DatabaseError as dberr:
        print("\nDatabase error - " + str(dberr) + "\n")
        raise SystemExit

    except Exception as err:
        raise Exception("\nError working with database - " + str(err))
    try:
        load_vaccine_doses(connection,vaccine_doses_url)
        load_cases_by_vacc_status(connection,cases_by_vacc_status_ont_url)
    except Exception as er:
        print("\nError raised - " + str(er) + "\n")

###############################################################################
# Execute Main
###############################################################################
main_process()