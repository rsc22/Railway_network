# -*- coding: utf-8 -*-
"""
Created on Sun Nov 22 23:13:45 2020

@author: remns

Scrape the schedule information from Renfe to obtain the stops of each route

-TODO: instead of sending the whole stops DataFrame, send just the direction
to connect to the SQL database so that it is not necessary to have the whole dataset loaded.

-TODO: CHECK other TODOS in the code

-TODO: a class to translate the rows read from the SQL database to useful data.
"""

import os
import os.path
import sys
import time
import datetime
from datetime import timedelta

import pandas as pd
import numpy as np
import selenium
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
import sqlite3
import json

from preprocess_datasets import *
from funcs import *

driver_path = r'C:\bin\chromedriver93.exe'

# Date and time format to use
dateformat = '%d/%m/%Y %H:%M'

#Select correct date for the search: tomorrow
tomorrow = datetime.datetime.today() + timedelta(days=1)
year, month, day = str(tomorrow.year), str(tomorrow.month), str(tomorrow.day)
    
def set_driver_path(path):
    global driver_path
    driver_path = path
    return driver_path

#url = r'https://horarios.renfe.com/HIRRenfeWeb/estaciones.do?&ID=s&icid=VTodaslasEstaciones'
#url= r'https://horarios.renfe.com/HIRRenfeWeb/destinos.do?&O='+\
    #depart_id + r'&ID=s&DF=' + day + r'&MF=' + month + r'&AF=' + year

#Definition of functions that operate with the classes in this module

def scrape_all(routes, stations, filter_by='MD', dbName='testdb1'):
    """Scrape all the routes contained in the DataFrame "routes",
    filtered by the service type indicated and create a new DB
    containing the scraped routes' information
    TODO: be aware that there is a counter to limit the number of
    routes scraped, just for testing.
    TODO: the function is yet to be tested"""
    if filter_by:
        routes = routes[routes.route_short_name == filter_by]
    dbMan = DBManager(dbName)
    dbMan.create_table()
    count = 0
    for row in routes.itertuples():
        routeObj = RouteCollection(row[1], row[2], stations)
        dbMan.add_from_routeObj(routeObj)
        count += 1
        if count >10:
            break

class ScheduleScraper:
    """Schedules of all the routes departing from the given city.
    Routes must be a dictionary containing route codes as keys and
    their respective type of service. For example: {'3141208223VRX': 'MD'}"""
    def __init__(self, depart, routes, stops, stations):
        self.__routes = routes
        self.__stops = stops
        self.__stations = stations.loc[:,['simple_loc', 'article']]
        self.__depart_city, self.__destin_city = None, None
        self.year, self.month, self.day = str(tomorrow.year), str(tomorrow.month), str(tomorrow.day)
        self.check_depart(str(depart))
        self.complete_routes_request()  # Creates self.__destin_id and self.__destin_city

    def check_depart(self, depart):
        route_codes = list(self.__routes.keys())
        if all(code[0:5] == depart for code in route_codes):
            pass
        else:
            depart_codes = [code[0:5] for code in route_codes]
            mistakes = list(set(depart_codes).difference(depart))
            mistaken_positions = [depart_codes.index(mistake) for mistake in mistakes]
            print('Some routes don\'t match the indicated depart point\n')
            for pos in mistaken_positions:
                print(route_codes[pos])
        self.__depart_id = depart
        self.__depart_name = self.__stops[self.__stops['stop_id'] == depart].iloc[0]['stop_name']

    def complete_routes_request(self):
        """Find the IDs for depart and destin in the route. Pair them with their
        correspondent name in stops dataset"""
        for route, service in self.__routes.items():
            if route[0:5] == self.__depart_id:
                depart_id, destination_id = route[0:5], route[5:10]
                #destination_name = self.__stops[self.__stops['stop_id'] == destination].iloc[0]['stop_name']
                depart_city = self.get_depart_city(depart_id)
                dest_city = self.get_destin_city(destination_id)
                self.__routes[route] = list((service, depart_city, depart_id, dest_city, destination_id))

    def set_depart_city(self, idnum):
        """Get the name of the city from the stations dataset"""
        idnum = str(idnum)
        if self.__stations.loc[idnum, 'article']:
            self.__depart_city = list(self.__stations.loc[idnum,\
                                                     ['simple_loc', 'article']])
        else:
            self.__depart_city = self.__stations.loc[idnum,\
                                                     'simple_loc']
    
    def set_destin_city(self, idnum):
        """Get the name of the city from the stations dataset"""
        idnum = str(idnum)
        if self.__stations.loc[idnum, 'article']:
            self.__destin_city = list(self.__stations.loc[idnum,\
                                                     ['simple_loc', 'article']])
        else:
            self.__destin_city = self.__stations.loc[idnum,\
                                                     'simple_loc']
    
    def get_depart_city(self, idnum):
        """Return the depart city name."""
        #if not hasattr(self, '__depart_city'):
         #   self.set_depart_city()
        if not self.__depart_city:
            self.set_depart_city(idnum)
        return self.__depart_city

    def get_destin_city(self, idnum):
        """Return the destination city name."""
        if not self.__destin_city:
            self.set_destin_city(idnum)
        return self.__destin_city

    def get_routes(self):
        return self.__routes
        
    def get_id(self, point='depart'):
        if point == 'depart':
            return self.__depart_id
        elif point == 'destin':
            return self.__destin_id

    def get_depart_name(self, cityreq=True):
        """Depart name as registered in the stops dataset.
        Optionally depart city name as registered in the stations dataset."""
        city_name = self.get_depart_city()
        return (self.__depart_name, city_name) if cityreq else self.__depart_name
        
    def get_routes_options(self, times=False):
        try:
            return self.__routes_options
        except AttributeError:
            self.set_routes_options(times=times)
            return self.__routes_options
        
    def set_routes_options(self, times=False):
        self.__routes_options = {}
        
        for route, info in self.__routes.items():
            self.__routes_options[route] = \
                self.scrape_routes_destination(info[0],info[1], info[2], info[3], info[4], times=times)
        print(self.__routes_options[route])
            
    def scrape_routes_destination(self, service, depart_city, depart_id, destin_city, destin_id, times=False):
        """Scrape routes from the depart to the destination provided, only
        for the service type indicated."""
        driver = webdriver.Chrome(executable_path=driver_path)
        destin_code = self.check_station_uniqueness(driver, destin_city)
        depart_code = self.check_station_uniqueness(driver, depart_city)
        destin = destin_code if destin_code else destin_id
        depart = depart_code if depart_code else depart_id
        url = r'https://horarios.renfe.com/HIRRenfeWeb/buscar.do?O='+\
            depart + r'&D=' + destin + r'&ID=s&AF=' + self.year +\
                r'&MF=' + self.month + r'&DF=' + self.day + r'&SF=4'
        driver.get(url)
        content_el = driver.find_elements_by_xpath('//td[@class="txt_borde1 irf-travellers-table__td"]')
        main_win = driver.window_handles[0]   
        local_routes = {}
        for element in content_el:
            try:
                hyperlink = element.find_element_by_xpath('.//*[contains(@href,"recorrido")]')
                route_name = hyperlink.text
                if service in route_name:
                    hyperlink.click()
                    driver.switch_to.window(driver.window_handles[-1])
                    #Select elements that form the timetable rows
                    table_els = driver.find_elements_by_xpath('//tr[@class="irf-renfe-travel__tr"]')[1:]
                    route_stops = {}
                    #Loop over the table row by row
                    for row in table_els:
                        cells = row.find_elements_by_xpath('.//td')
                        route_stops[cells[0].text] = [cells[1].text, cells[2].text]
                    local_routes[route_name] = route_stops
                    driver.close()
                    driver.switch_to.window(main_win)
                else:
                    continue
            except:
                continue
        print(url)
        driver.quit()
        if not times:
            for route in local_routes.keys():
                print(route)
                local_routes[route] = list(local_routes[route].keys())
        return local_routes
    
    def check_station_uniqueness(self, driver, city):
        """Find whether the city has more than one station. In the stations web menu
        these have the name CITYNAME(TODAS)"""
        citycheck = city
        code = None
        url = r'https://horarios.renfe.com/HIRRenfeWeb/estaciones.do?&ID=s&icid=VTodaslasEstaciones'
        driver.get(url)
        if type(citycheck) is list:
            letter_block = citycheck[-1][0]
            cityname = citycheck[0]
        else:
            letter_block = citycheck[0]
            cityname = citycheck
        print(letter_block)
        xpath = r'//*[contains(@headers,"letra ' + letter_block + r'")]'
        station_options = driver.find_element_by_xpath(xpath).text
        station_options_list = station_options.split('\n')
        for name_length in range(len(cityname), 0, -1):
            try:
                matches = [city for city in station_options_list\
                      if cityname[0:name_length] in city]
                if len(matches) > 0:
                    for match in matches:
                        if 'TODAS' in match:
                            code = match[0:5]
                            code = code.replace(' ', '-')
                    break
            except:
                pass
        return code
    
    def clean_duplicated(self):
        """If we are just interested in the unique route possibilities, delete
        repeated combinations of stops, regardless of their time of the day"""
        #if hasattr(self, '__routes_options'):
        try:
            convert = False if type(list(list(self.__routes_options.values())[0]\
                                         .values())[0]) is list else True
            clean = {}
            if not convert: #Stops are already a list
                for connection, routes in self.__routes_options.items(): #Lop over each route_id
                    #For a given route_id, there are different possible routes.
                    #Put them in Series format (Index are the route ID and Values are the
                    #list of stops)
                    routes_data = pd.Series(routes)
                    #Delete duplicate alternatives to the route (same stops)
                    routes_data = pd.Series(np.unique(routes_data))
                    clean[connection] = routes_data
            else:   #Stops are keys of a dictionary
                for connection, routes in self.__routes_options.items(): #Lop over each route_id
                    simple_routes = {}
                    simple_routes = {k: list(v.keys()) for (k, v) in routes.items()}
                    #for route, stops in routes.items():
                     #   simple_routes[route] = list(stops.keys())
                    routes_data = pd.Series(simple_routes)
                    #Delete duplicate alternatives to the route (same stops)
                    routes_data = pd.Series(np.unique(routes_data))
                    clean[connection] = routes_data
            return routes_data
        except AttributeError:
            print('Please perform first the scrape (this has not been automated\
                  in order to keep readability of the code)\n')

    def routes_to_dataframe(self):
        """Only works if the __routes_options dictionary has the times"""
        temp_routes ={}
        for route, options in self.__routes_options.items():
            temp_routes[route] = {}
            for option, stops in options.items():
                temp_routes[route][option] = {}
                temp_routes[route][option]['stops'] = list(stops.keys())
                temp_routes[route][option]['times'] = list(stops.values())
        return temp_routes
    
    def temporary_setter(self, inp):
        self.__routes_options = inp

    
class RouteCollection:
    """Object corresponding to just one route ID"""
    def __init__(self, route_id, serv_type, stations):
        self.__route = route_id
        self.__type = serv_type
        self.stations = stations
        self.__depart, self.__destin = self.complete_route_data()
    
    def get_route_id(self):
        return self.__route
    
    def get_route_type(self):
        return self.__type

    def complete_route_data(self):
        """Obtain the depart and destination stations: their ID and name of the city
        The city name is resturned as a list, where the element in index 1 is the article in the case
        of coumpund-name cities and None in single-name cities."""
        depart, destination = self.__route[0:5], self.__route[5:10]
        depart_name = list(self.stations.loc[depart, ['simple_loc', 'article']])
        destination_name = list(self.stations.loc[destination, ['simple_loc', 'article']])
        return {'point': 'depart', 'id': depart, 'name': depart_name},\
            {'point': 'destination', 'id': destination, 'name': destination_name}

    def get_route_info(self):
        """Return depart and destination info dictionaries"""
        return self.__depart, self.__destin
    
    def get_route_options(self, times=True):
        """Returns all the routes' information. If they weren't scraped already,
        it executes the scrape"""
        local_routes = {}
        if not hasattr(self, 'scraped'):
            self.scrape_trips()
        if not times:
            for route in self.__route_options.keys():
                local_routes[route] = list(self.__route_options[route].keys())
            return local_routes
        else:
            return self.__route_options

    def scrape_trips(self):
        """Main scraping method"""
        self.scraper = TripScraper(self.__depart, self.__destin)
        self.scraper.start_driver()
        unique_depart = self.unique_station('depart', self.scraper.driver)
        unique_destin = self.unique_station('destination', self.scraper.driver)
        depart = self.__depart['id'] if unique_depart else self.__depart['name']
        destin = self.__destin['id'] if unique_destin else self.__destin['name']
        self.scraper.goto_horarios(depart, destin, year, month, day)
        self.__route_options = self.scraper.scrape_route_options(self.__type)
        self.scraped = True

    def clean_duplicated(self):
        """If we are just interested in the unique route possibilities, delete
        repeated combinations of stops, regardless of their time of the day.
        TODO: STORE in some variable the list of route option codes that correspond to
        each unique route possibility."""
        #if hasattr(self, '__route_options'):
        try:
            convert = False if type(list(list(self.__route_options.values())[0]\
                                         .values())[0]) is list else True
            clean = {}
            simple_routes = {k: list(v.keys()) for (k, v) in self.__route_options.items()}
            #for route, stops in routes.items():
                #   simple_routes[route] = list(stops.keys())
            routes_data = pd.Series(simple_routes)
            #Delete duplicate alternatives to the route (same stops)
            routes_data = pd.Series(np.unique(routes_data))
            return routes_data
        except AttributeError:
            print('Please perform first the scrape (this has not been automated\
                  in order to keep readability of the code)\n')

    def routes_to_dataframe(self):
        """Return the routes options in DataFrame format.
        "stops" and "times" are the columns and their elements are the corresponding
        lists."""
        if not hasattr(self, 'scraped'):
            self.scrape_trips()
        temp_routes ={}
        for option, stops in self.__route_options.items():
            option, serviceType = option.split()
            temp_routes[option] = {}
            temp_routes[option]['stops'] = list(stops.keys())
            temp_routes[option]['times'] = list(stops.values())
            temp_routes[option]['service'] = serviceType
        return pd.DataFrame.from_dict(temp_routes, orient='index')
    
    def compress_information(self, json_out=False, dict_repr=False):
        """Returns the compressed representation. It consists of the unique route
        options paired with the route_option codes that offer that option."""
        unique_routes = self.clean_duplicated()
        df = self.routes_to_dataframe()
        #if json_out:
        #    pass
        #else:
        compressed = {}
        for idx, route in enumerate(unique_routes):
            compressed[idx] = repr([route, df[df['stops'].map(len) == len(route)].index.tolist(), list(df[df['stops'].map(len) == len(route)]['times'])])
        return repr(compressed) if dict_repr else compressed

    def unique_station(self, point, driver):
        """Find whether the city has more than one station. In the stations web menu
        these have the name CITYNAME(TODAS)"""
        if point == self.__depart['point']:
            name = self.__depart['name']
        elif point == self.__destin['point']:
            name = self.__destin['name']
        unique = True
        url = r'https://horarios.renfe.com/HIRRenfeWeb/estaciones.do?&ID=s&icid=VTodaslasEstaciones'
        driver.get(url)
        # Cities are grouped by their initial letter. If they have a compound name,\
        # their block depends on their article or first word
        letter_block = name[0][0] if not name[1] else name[1][0]
        city = name[0]
        xpath = r'//*[contains(@headers,"letra ' + letter_block + r'")]'
        station_options = driver.find_element_by_xpath(xpath).text
        station_options_list = station_options.split('\n')
        for name_length in range(len(city), 0, -1):
            try:
                matches = [found_city for found_city in station_options_list\
                      if city[0:name_length] in found_city]
                if len(matches) > 0:
                    for match in matches:
                        if 'TODAS' in match:
                            unique = False
                    break
            except:
                pass
        return unique

    def temporary_setter(self, inp):
        self.__route_options = inp


class TripScraper:
    def __init__(self, depart_dict, destin_dict):
        self.__depart = depart_dict
        self.__destin = destin_dict
        self.__options_table = None

    def start_driver(self):
        """Open the driver and save the main window"""
        self.driver = webdriver.Chrome(executable_path=driver_path)
        self.main_win = self.driver.window_handles[0]
    #####
    def goto_horarios(self, depart, destin, year, month, day):
        """Go to the page with the list of routes options and store the element table that contains
        the links"""
        depart, destin = self.format_entries(depart, destin)
        url = r'https://horarios.renfe.com/HIRRenfeWeb/buscar.do?O='+\
                depart + r'&D=' + destin + r'&ID=s&AF=' + year +\
                    r'&MF=' + month + r'&DF=' + day + r'&SF=0'
        try:
            self.driver.get(url)
            self.__options_table = self.driver.find_elements_by_xpath('//td[@class="txt_borde1 irf-travellers-table__td"]')
        except:
            """If the url drove to a mistaken page, return to stations menu and click links"""
            self.goto_estaciones()
            self.click_city(self.__depart['name'], True)
            self.click_city(self.__destin['name'], False)
            self.__options_table = self.driver.find_elements_by_xpath('//td[@class="txt_borde1 irf-travellers-table__td"]')

    def goto_estaciones(self):
        url = r'https://horarios.renfe.com/HIRRenfeWeb/estaciones.do?&ID=s&icid=VTodaslasEstaciones'
        self.driver.get(url)

    def click_city(self, name, dates=True):
        todas = False   #Flag to check whether it was a multistation city or not
        if dates:
            self.manual_dates()
        letter_block = name[0][0] if not name[1] else name[1][0]
        city = name[0]
        # Find whether the city is in the HTML text and find its correspondent element
        xpath = r'//*[contains(@headers,"letra ' + letter_block + r'")]'
        station_options = self.driver.find_element_by_xpath(xpath).text
        station_options_list = station_options.split('\n')

        for name_length in range(len(city), 0, -1):
            try:
                matches = [found_city for found_city in station_options_list\
                      if city[0:name_length] in found_city]
                if len(matches) > 0:
                    for match in matches:
                        if 'TODAS' in match:
                            #Find position of the link element in the list of its letter block
                            city_index = station_options_list.index(match)
                            todas = True
                    if not todas:   #There is only one station in the city and hence we expect only one result
                        city_index = 0
                    break
            except:
                pass
        xpath = xpath + r'//a[@class="linkgrise irf-travellers-table__tbody-lnk"]'
        station_elements = self.driver.find_elements_by_xpath(xpath)
        station_elements[city_index].click()

    def manual_dates(self):
        day_dropdown = self.driver.find_element_by_id('DF')
        select_day = day_dropdown.find_element_by_xpath('.//option[@value="' + str(day) + r'"]')
        select_day.click()
        month_dropdown = self.driver.find_element_by_id('MF')
        select_month = month_dropdown.find_element_by_xpath('.//option[@value="' + str(month) + r'"]')
        select_month.click()
        year_dropdown = self.driver.find_element_by_name('AF')
        select_year = year_dropdown.find_element_by_xpath('.//option[@value="' + str(year) + r'"]')
        select_year.click()

    def format_entries(self, depart, destin):
        try:
            int(depart)
        except:
            depart = self.format_name(depart[0])
        try:
            int(destin)
        except:
            destin = self.format_name(destin[0])
        return depart, destin

    def format_name(self, name):
        if len(name) > 4:
            return name[0:5]
        else:
            return name + '-' * (5 - len(name))
    #####
    def scrape_route_options(self, service):
        local_routes = {}
        assert self.__options_table, "It is necessary to call first the method\
            .goto_horarios() with the proper arguments"
        for element in self.__options_table:
            try:
                hyperlink = element.find_element_by_xpath('.//*[contains(@href,"recorrido")]')
                route_name = hyperlink.text
                if service in route_name:
                    hyperlink.click()
                    self.driver.switch_to.window(self.driver.window_handles[-1])     #Activate pop up window with the stops of the route option
                    #Select elements that form the timetable rows
                    table_els = self.driver.find_elements_by_xpath('//tr[@class="irf-renfe-travel__tr"]')[1:]
                    route_stops = {}
                    #Loop over the table row by row
                    for row in table_els:
                        cells = row.find_elements_by_xpath('.//td')
                        route_stops[cells[0].text] = [cells[1].text, cells[2].text]
                    local_routes[route_name] = route_stops
                    self.driver.close()
                    self.driver.switch_to.window(self.main_win)
                else:
                    continue
            except:
                continue
        self.driver.quit()
        return local_routes
    
    def quit(self):
        self.driver.quit()

    def get_points(self):
        """Return the depart and destin information contained in this class"""
        return self.__depart, self.__destin
    

class DFManager:
    """Class to easily handle the csv files containing the data and manipulate
    these registers using DataFrames"""
    def __init__(self, name):
        self.__folder, self.work_dir = self.df_folder()
        self.__name = name

    def df_folder(self):
        """Return the folder where the CSV files will be saved. It must be
        within the main folder (the one containing the scripts), so it is
        created in case it doesn't exist"""
        work_dir = os.getcwd()
        if not os.path.exists(work_dir + r'\csv_regs'):
            new_folder(r'csv_regs')
        return work_dir + r'\csv_regs', work_dir

    def create_register(self):
        """Create a new routes register from scratch"""
        self.reg = pd.DataFrame(columns=['ID', 'Depart', 'Destination', 'Service_Type', 'Routes']).set_index('ID')

    def close(self):
        """Use this method when finished working with the current DF. A csv file is saved
        with the current content of the reg attribute"""
        filepath = self.__folder + r'\\' + self.__name
        self.reg.to_csv(filepath + '.csv', sep=';')

    def open(self):
        """Open existing register csv file"""
        filepath = self.__folder + r'\\' + self.__name
        self.reg = pd.read_csv(filepath + '.csv', sep=';').set_index('ID')

    def add_route(self, collection, overwrite=False):
        """Adds a row to the register, corresponding to the route given as input.
        If overwrite=True, already existing rows are overwritten"""
        if not hasattr(self, 'reg'):
            try:
                self.open()
            except:
                self.create_register()
        name1 = self.rebuild_name(collection.get_route_info()[0]['name'])
        name2 = self.rebuild_name(collection.get_route_info()[1]['name'])
        input_data = [collection.get_route_id(), name1, name2, collection.get_route_type(), collection.compress_information(dict_repr=True)]
        self.reg.loc[input_data[0]] = input_data[1:]

    def rebuild_name(self, in_name):
        """Method to rebuild the depart and destination city names.
        They come stored as lists"""
        if in_name[1]:
            return in_name[1] + ' ' + in_name[0]
        else:
            return in_name[0]


class DBManager:
    def __init__(self, name, tableName=None):
        self.__folder, self.work_dir = self.db_folder()
        self.__name = name
        self.__tableName = 'Routes' if (not tableName) else tableName
        self.__table_exists = False

    def db_folder(self):
        """Return the folder where the DataBases will be saved. It must be
        within the main folder (the one containing the scripts), so it is
        created in case it doesn't exist"""
        work_dir = os.getcwd()
        if not os.path.exists(work_dir + r'\databases'):
            new_folder(r'databases')
        return work_dir + r'\databases', work_dir

    def connect(self):
        """Stablish connection to the database assigned to the
        object"""
        self.__conn = sqlite3.connect(self.__folder + "/" + self.__name + '.db')

    def close(self):
        """Close connection currently active for the DB object"""
        self.__conn.close()

    def create_table(self):
        """Create the table in the database"""
        self.connect()
        cursor = self.__conn.cursor()
        creation_string = " CREATE TABLE IF NOT EXISTS " + self.__tableName + " (route_id integer NOT NULL,\
                                        service text NOT NULL,\
                                        line_code text NOT NULL,\
                                        depart text NOT NULL,\
                                        destination text NOT NULL,\
                                        stops text NOT NULL,\
                                        times text NOT NULL\
                                    ); "
        cursor.execute(creation_string)
        self.__table_exists = True
        
    def check_table(self, tableName, verbose=False):
        """Checks whether a given table already exists"""
        exists = False
        c = self.__conn.cursor()
        c.execute(" SELECT count(name) FROM sqlite_master WHERE type='table' AND name= ?", (tableName,))
        #if the count is 1, then table exists
        if c.fetchone()[0]==1:
            exists = True
        if verbose and exists:
            print('Table already exists\ n')
        #commit the changes to db			
        self.__conn.commit()
        return exists

    def check_route(self):
        """Checks whether the row for this route already exists"""
        return

    def add_from_routeObj(self, routeObj):
        """Adds all the routes contained in the RouteCollection Object received"""
        self.connect()
        df = routeObj.routes_to_dataframe()
        depart, dest = routeObj.get_route_info()
        depart, dest = self.rebuild_name(depart['name']), self.rebuild_name(dest['name'])
        linecode = routeObj.get_route_id()
        #for row in df.itertuples():
        for rowidx in range(df.shape[0]):
            self.add_route([df.iloc[rowidx,:], linecode, depart, dest])
        self.close()

    def add_route(self, route_data):
        """Adds a new row to the table
        TODO: right now it takes the data from DataFrame in a specific order.
        It would be better to pick the data using column names"""
        cursor = self.__conn.cursor()
        infos, linecode, depart, dest = route_data
        route_id = int(infos.name)
        stops, times, service = infos.iloc[:].values
        stopstring, timesstring = repr(stops), repr(times)
        sql = ''' INSERT INTO Routes (route_id,service,line_code,depart,destination,stops,times)
              VALUES(?,?,?,?,?,?,?) '''
        cursor.execute(sql, (route_id, service, linecode, depart, dest, stopstring, timesstring))
        self.__conn.commit()
        return
    
    def update_route(self):
        """Updates an existing row. TODO"""
        return

    def query_route(self, routeID):
        """Query a single row from the database given its individual
        route ID."""
        self.connect()
        cur = self.__conn.cursor()
        statement = "SELECT * FROM Routes WHERE route_id=?"
        cur.execute(statement, (routeID,))
        row = cur.fetchall()
        self.close()
        return row

    def select_all_routes(self):
        """Query all rows in the Routes table -> list of rows (tuples)"""
        self.connect()
        cur = self.__conn.cursor()
        cur.execute("SELECT * FROM Routes")
        rows = cur.fetchall()
        self.close()
        return rows

    def select_route_by_service(self, serviceName):
        """Query routs of a given service type"""
        try:
            cur = self.__conn.cursor()
            cur.execute("SELECT * FROM Routes WHERE service=?", (serviceName,))
            rows = cur.fetchall()
        except:
            if not self.__table_exists:
                print('\nThe table does not exist in the database yet.\
                    Create it first using the method create_table().\n')

    def custom_query(self, columns=['*'], filters=None, operator='AND'):
        """Pass de adequate arguments to specify a query.
        *Colummns to return: list of the column names. Defaults to '*'
        *Filters: dictionary containing column:[operator, value]
            operator must be a string ("=", "!=", "<", etc.)
        *Filters combination: only supports one operator that is
            applied to combine all filters (default to AND).
        TODO: the filters loop can be definitely improved:
            -Better way to write the right amount of logical ops.
            -Actually passing an argument with the logical ops.
            -Build the statement with the ?,?,? notation if possible.
        """
        statement = ''
        select_state = "SELECT " + columns[0]
        try:
            for column in columns[1:]:
                select_state = select_state + ', ' + column
        except:
            pass
        statement = select_state + ' FROM ' + self.__tableName
        if filters:
            statement = statement + ' WHERE '
            many = True if len(list(filters.keys())) > 1 else False
            for col, filt in filters.items():
                statement = statement + col + ' ' + filt[0] + ' ' + str(filt[2])
                statement = statement + ' ' + operator
            #Now delete the last operator (this process should be improved):
            statement = statement.rsplit(' ', 1)[0]
            print(statement)
        self.connect()
        cur = self.__conn.cursor()
        cur.execute(statement)
        rows = cur.fetchall()
        self.close()
        return rows

    def rebuild_name(self, in_name):
        """Method to rebuild the depart and destination city names.
        They come stored as lists"""
        if in_name[1]:
            return in_name[1] + ' ' + in_name[0]
        else:
            return in_name[0]



if __name__ == '__main__':
    routes, stops, stations = preprocess_data()
    #Set the working directory to the one where the script is placed.
    os.chdir(get_script_path())

    """
    #test = ScheduleScraper('31412', {'3141208223VRX': 'MD'}, stops, stations.loc[:,['simple_loc', 'article']])
    test = ScheduleScraper('71801', {'7180113200GL023': 'ALVIA'}, stops, stations.loc[:,['simple_loc', 'article']])    
    result = test.get_routes_options(times=True)
    #clean=test.clean_duplicated()
    """