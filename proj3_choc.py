import sqlite3
import csv
import json

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

def drop_table(conn, cur):
    # Drop tables
    statement = '''
        DROP TABLE IF EXISTS 'Bars';
    '''
    cur.execute(statement)
    statement = '''
        DROP TABLE IF EXISTS 'Countries';
    '''
    cur.execute(statement)
    conn.commit()


def create_table(conn, cur):
    drop_table(conn, cur)

    statement = '''
        CREATE TABLE 'Bars'(
            'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Company' TEXT NOT NULL,
            'SpecificBeanBarName' TEXT NOT NULL,
            'REF' TEXT NOT NULL,
            'ReviewDate' TEXT,
            'CocoaPercent' REAL,
            'CompanyLocationId' INTEGER NOT NULL,
            'Rating' REAL,
            'BeanType' TEXT,
            'BroadBeanOriginId' INTEGER,
            FOREIGN KEY(CompanyLocationId) REFERENCES Countries(Alpha3),
            FOREIGN KEY(BroadBeanOriginId) REFERENCES Countries(Alpha3)
        ) 
    '''
    cur.execute(statement)

    statement = '''
        CREATE TABLE 'Countries'(
            'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Alpha2' TEXT NOT NULL,
            'Alpha3' TEXT NOT NULL,
            'EnglishName' TEXT UNIQUE NOT NULL,
            'Region' TEXT,
            'Subregion' TEXT NOT NULL,
            'Population' INTEGER,
            'Area' REAL
        )
    '''
    cur.execute(statement)


# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'

conn = sqlite3.connect('choc.db')
cur = conn.cursor()

create_table(conn, cur)

json_file = open(COUNTRIESJSON, 'r', encoding='utf-8')
COUNTRIES_DATA = json.loads(json_file.read())
#print(COUNTRIES_DATA)
for country in COUNTRIES_DATA:
    name = country['name']
    alpha2 = country['alpha2Code']
    alpha3 = country["alpha3Code"]
    region = country['region']
    subregion = country["subregion"]
    population = country["population"]
    area = country["area"]

    insertion = (alpha2, alpha3, name, region, subregion, population, area)

    statement = '''
        INSERT INTO Countries(Alpha2, Alpha3, EnglishName, Region, Subregion, Population, Area)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    '''
    cur.execute(statement, insertion)


with open('flavors_of_cacao_cleaned.csv', encoding='utf-8') as csvDataFile:
    csvReader = csv.reader(csvDataFile)
    for row in csvReader:
        if row[0] != 'Company':
            #print(row)

            statement = '''
                SELECT Countries.Id FROM Countries
                WHERE Countries.EnglishName=?
            '''
            cur.execute(statement, (row[5], ))
            countryId = cur.fetchone()
            #print(countryId)
            if countryId != None:
                countryId = countryId[0]

            statement = '''
                            SELECT Countries.Id FROM Countries
                            WHERE Countries.EnglishName=?
                        '''
            cur.execute(statement, (row[8], ))
            origionId = cur.fetchone()
            #print(countryId)
            if origionId != None:
                origionId = origionId[0]

            if row[4] != None:
                cocoaPercent = float(row[4][:-1]) / 100

            statement = '''
                INSERT INTO Bars
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            cur.execute(statement, (None, row[0], row[1], row[2], row[3], cocoaPercent, countryId, row[6], row[7], origionId))

conn.commit()
conn.close()


# Part 2: Implement logic to process user commands
def process_command(command):
    command_list = command.split()
    return_list = []
    command_option = command_list[0]
    sort_param = 'b.Rating'
    order_param = 'DESC'
    limit_param = 10
    group_param = 'b.CompanyLocationId'
    group_statement = " GROUP BY {} HAVING COUNT(SpecificBeanBarName)>4"

    if command_option == 'bars':
        statement = '''
                        SELECT b.SpecificBeanBarName, b.Company, c.EnglishName, b.Rating, b.CocoaPercent, c2.EnglishName
                        FROM Bars AS b
                            JOIN Countries AS c ON b.CompanyLocationId = c.Id
                                JOIN Countries AS c2 ON b.BroadBeanOriginId = c2.Id
                    '''
        if len(command_list) >1:
            for i in command_list[1:]:
                if 'sellcountry' in i:
                    alpha2 = i[-2:]
                    statement += " WHERE c.Alpha2='{}'".format(alpha2)
                elif 'soucecountry' in i:
                    alpha2 = i[-2:]
                    statement += " WHERE c2.Alpha2='{}'".format(alpha2)
                elif 'sellregion' in i:
                    region_param = i.split('=')[1]
                    statement += " WHERE c.Region='{}'".format(region_param)
                elif 'sourceregion' in i:
                    region_param = i.split('=')[1]
                    statement += " WHERE c2.Region='{}'".format(region_param)

                elif 'ratings' in i:
                    continue
                elif 'cocoa' in i:
                    sort_param = "B.CocoaPercent"

                elif 'top' in i:
                    limit_param = i.split('=')[1]
                elif 'bottom' in i:
                    limit_param = i.split('=')[1]
                    order_param = "ASC"

                else:
                    return


        statement += " ORDER BY {} {} LIMIT {}".format(sort_param, order_param, limit_param)

    elif command_option == 'companies':
        statement = '''
                        SELECT b.Company, c.EnglishName, {}
                        FROM Bars AS b
                            JOIN Countries AS c ON b.CompanyLocationId = c.Id
                    '''
        if len(command_list) >1:
            for i in command_list[1:]:
                if 'region' in i:
                    region_param = i.split('=')[1]
                    statement += " WHERE c.Region='{}'".format(region_param)
                elif 'country' in i:
                    alpha2 = i[-2:]
                    statement += " WHERE c.Alpha2='{}'".format(alpha2)

                elif 'ratings' in i:
                    continue
                elif 'cocoa' in i:
                    sort_param = "b.CocoaPercent"
                elif 'bars_sold' in i:
                    sort_param = "COUNT(SpecificBeanBarName)"

                elif 'top' in i:
                    limit_param = i.split('=')[1]
                elif 'bottom' in i:
                    limit_param = i.split('=')[1]
                    order_param = "ASC"
                else:
                    return


        if sort_param != 'COUNT(SpecificBeanBarName)':
            statement = statement.format('AVG(' + sort_param + ')')
        else:
            statement = statement.format(sort_param)

        statement += group_statement.format('b.Company')
        #print(sort_param)

        if sort_param != 'COUNT(SpecificBeanBarName)':
            statement += " ORDER BY AVG({}) {} LIMIT {}".format(sort_param, order_param, limit_param)
        else:
            statement += " ORDER BY {} {} LIMIT {}".format(sort_param, order_param, limit_param)


    elif command_option == 'countries':
        statement = '''
                        SELECT c.EnglishName, c.Region, {}
                        FROM Bars AS b
                            JOIN Countries AS c ON {} = c.Id
                    '''
        if len(command_list) >1:
            for i in command_list[1:]:
                if 'region' in i:
                    region_param = i.split('=')[1]
                    statement += " WHERE c.Region='{}'".format(region_param)

                elif 'sources' in i:
                    group_param = 'b.BroadBeanOriginId'
                elif 'sellers' in i:
                    continue

                elif 'ratings' in i:
                    continue
                elif 'cocoa' in i:
                    sort_param = "c.CocoaPercent"
                elif 'bars_sold' in i:
                    sort_param = "COUNT(SpecificBeanBarName)"

                elif 'top' in i:
                    limit_param = i.split('=')[1]
                elif 'bottom' in i:
                    limit_param = i.split('=')[1]
                    order_param = "ASC"
                else:
                    return

        if sort_param != 'COUNT(SpecificBeanBarName)':
            statement = statement.format('AVG(' + sort_param + ')', group_param)
        else:
            statement = statement.format(sort_param, group_param)

        statement += group_statement.format(group_param)

        if sort_param != 'COUNT(SpecificBeanBarName)':
            statement += " ORDER BY AVG({}) {} LIMIT {}".format(sort_param, order_param, limit_param)
        else:
            statement += " ORDER BY {} {} LIMIT {}".format(sort_param, order_param, limit_param)


    elif command_option == 'regions':
        statement = '''
                        SELECT c.Region, {}
                        FROM Bars AS b
                            JOIN Countries AS c ON {} = c.Id
                    '''

        if len(command_list) >1:
            for i in command_list[1:]:
                if 'sources' in i:
                    group_param = 'b.BroadBeanOriginId'
                elif 'sellers' in i:
                    continue

                elif 'ratings' in i:
                    continue
                elif 'cocoa' in i:
                    sort_param = "c.CocoaPercent"
                elif 'bars_sold' in i:
                    sort_param = "COUNT(SpecificBeanBarName)"

                elif 'top' in i:
                    limit_param = i.split('=')[1]
                elif 'bottom' in i:
                    limit_param = i.split('=')[1]
                    order_param = "ASC"
                else:
                    return

        if sort_param != 'COUNT(SpecificBeanBarName)':
            statement = statement.format('AVG(' + sort_param + ')', group_param)
        else:
            statement = statement.format(sort_param, group_param)

        statement += group_statement.format('c.Region')

        if sort_param != 'COUNT(SpecificBeanBarName)':
            statement += " ORDER BY AVG({}) {} LIMIT {}".format(sort_param, order_param, limit_param)
        else:
            statement += " ORDER BY {} {} LIMIT {}".format(sort_param, order_param, limit_param)



    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    #print(statement)
    cur.execute(statement)
    return_list = cur.fetchall()

    conn.close()
    return return_list


def load_help_text():
    with open('help.txt') as f:
        return f.read()

# Part 3: Implement interactive prompt. We've started for you!
def print_results(results):
    for i in results:
        output_row = ''
        for j in i:
            if len(str(j)) > 12:
                output_row += "{:<15}".format(str(j)[:12] + '...') + ' '
            else:
                output_row += "{:<15}".format(str(j)) + ' '
        print(output_row)


def interactive_prompt():
    help_text = load_help_text()
    response = ''
    response_type = ["bars", "companies", "countries", "regions"]

    while response != 'exit':
        response = input('Enter a command: ')
        command_type = response.split()[0]

        if response == 'help':
            print(help_text)
            continue

        elif command_type in response_type:
            try:
                results = process_command(response)
                print_results(results)
                print()
            except:
                print("Command not recognized: " + response)

        elif response == 'exit':
            print('bye')

        else:
            print("Command not recognized: " + response)



# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    interactive_prompt()
