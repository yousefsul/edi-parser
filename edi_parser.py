import datetime
import os

import shortuuid

from connectMongoDB import ConnectMongoDB


def get_current_status():
    current_status = {
        "status": "new",
        "date": {
            "date": datetime.datetime.now().date().strftime("%Y%m%d"),
            "time": datetime.datetime.now().time().strftime("%H:%M:%S")
        }
    }
    return current_status


def generate_id():
    return int(shortuuid.ShortUUID(alphabet="0123456789").random(length=10))


class EdiParser:
    def __init__(self, edi_file):
        self.edi_file = edi_file
        self.segment, self.data_element = None, None
        self.index, self.count, self.count_st = 0, 1, 0
        self.time = datetime.datetime.now().time().strftime("%H:%M:%S")
        self.date = datetime.datetime.now().date().strftime("%Y%m%d")
        self.__id = generate_id()
        self.final_segment = {'header_section': {
            'trans_src_id': self.__id,
            'file_name': os.path.basename(edi_file),
            "date_created": {
                "date": self.time,
                "time": self.date
            },
            "current_status": get_current_status(),
            "status_history": [get_current_status()],
        }}
        self.info_837 = {
            '837_index': {
                'header_section': {
                    'trans_src_id': self.__id,
                    'file_name': os.path.basename(edi_file),
                    "date_created": {
                        "date": self.time,
                        "time": self.date
                    },
                    "current_status": get_current_status(),
                    "status_history": [get_current_status()],
                }
            }
        }
        self.connection = ConnectMongoDB()
        with open(self.edi_file, 'r') as edi_file:
            self.edi_file_info = edi_file.read().strip('~').split('~')

        for self.i in range(len(self.edi_file_info)):
            if self.edi_file_info:
                self.NM1 = 0
                self.HL = 0
                self.__extract_data()

        self.connection.connect_to_test_837_collection()
        self.connection.insert_to_test_837_collection(self.final_segment)

    def __pop_element(self, index):
        self.edi_file_info.pop(index)

    def __extract_data(self):
        self.data_element = self.edi_file_info[0].split('*')
        self.index += 1
        self.segment = self.data_element.pop(0) + '-' + str(self.index)
        self.bulid_main_dict()

    def bulid_main_dict(self):
        self.count = 1
        self.final_segment[self.segment] = {}
        if self.segment.split('-')[0] == 'ST':
            self.count_st += 1
            self.final_segment[self.segment]['status'] = 'pending'
            self.__bulid_data_element(self.final_segment[self.segment])
            self.__bulid_se_dict()
            self.__bulid_sub_dict()
        else:
            self.__bulid_data_element(self.final_segment[self.segment])

    def __bulid_se_dict(self):
        for i in range(len(self.edi_file_info)):
            try:
                if self.edi_file_info[i].split('*')[0] == 'SE':
                    data_element = self.edi_file_info[i].split('*')
                    self.index += 1
                    self.count = 1
                    segment = data_element.pop(0) + '-' + str(self.index)
                    self.final_segment[self.segment][segment] = {}
                    for data in data_element:
                        data_element_count = '{:02}'.format(self.count)
                        self.final_segment[self.segment][segment][data_element_count] = data
                        self.count += 1
                    self.__pop_element(i)
                    break
            except IndexError:
                pass

    def __bulid_sub_dict(self):
        for i in range(len(self.edi_file_info)):
            i = 0
            try:
                if self.edi_file_info[i].split('*')[0] == 'NM1':
                    if self.NM1 == 0:
                        self.NM1 += 1
                        self.__bulid_1000a_loop()
                        continue

                    elif self.NM1 == 1:
                        self.NM1 += 1
                        self.__bulid_1000b_loop()
                        continue

                if self.edi_file_info[i].split('*')[0] == 'HL':
                    if self.HL == 0:
                        self.HL += 1
                        self.__bulid_2000a_loop()
                        continue

                    if self.HL == 1:
                        self.HL += 1
                        self.__bulid_2000b_loop()
                        continue

                if self.edi_file_info[i].split('*')[0] == 'CLM':
                    self.__bulid_2300_loop()
                    break
                else:
                    self.data_element = self.edi_file_info[i].split('*')
                    self.index += 1
                    segment = self.data_element.pop(0) + '-' + str(self.index)
                    self.final_segment[self.segment][segment] = {}
                    self.__bulid_data_element(self.final_segment[self.segment][segment])
            except IndexError:
                pass

    def __bulid_1000a_loop(self):
        data_element = self.edi_file_info[0].split('*')
        self.index += 1
        self.count = 1
        segment = data_element.pop(0) + '-' + str(self.index)
        self.final_segment[self.segment]['1000A'] = {}
        self.final_segment[self.segment]['1000A'][segment] = {}
        for data in data_element:
            data_element_count = '{:02}'.format(self.count)
            self.final_segment[self.segment]['1000A'][segment][data_element_count] = data
            self.count += 1
        self.edi_file_info.pop(0)
        for i in range(len(self.edi_file_info)):
            i = 0
            try:
                if self.edi_file_info[i].split('*')[0] != 'NM1' or self.edi_file_info[i].split('*')[0] == 'HL':
                    data_element = self.edi_file_info[i].split('*')
                    self.index += 1
                    self.count = 1
                    segment = data_element.pop(0) + '-' + str(self.index)
                    self.final_segment[self.segment]['1000A'][segment] = {}
                    for data in data_element:
                        data_element_count = '{:02}'.format(self.count)
                        self.final_segment[self.segment]['1000A'][segment][data_element_count] = data
                        self.count += 1
                    self.__pop_element(0)
                else:
                    break
            except IndexError:
                pass

    def __bulid_1000b_loop(self):
        data_element = self.edi_file_info[0].split('*')
        self.index += 1
        self.count = 1
        segment = data_element.pop(0) + '-' + str(self.index)
        self.final_segment[self.segment]['1000B'] = {}
        self.final_segment[self.segment]['1000B'][segment] = {}
        for data in data_element:
            data_element_count = '{:02}'.format(self.count)
            self.final_segment[self.segment]['1000B'][segment][data_element_count] = data
            self.count += 1
        self.edi_file_info.pop(0)

    def __bulid_2000a_loop(self):
        data_element = self.edi_file_info[0].split('*')
        self.index += 1
        self.count = 1
        segment = data_element.pop(0) + '-' + str(self.index)
        self.final_segment[self.segment]['2000A'] = {}
        self.final_segment[self.segment]['2000A'][segment] = {}
        for data in data_element:
            data_element_count = '{:02}'.format(self.count)
            self.final_segment[self.segment]['2000A'][segment][data_element_count] = data
            self.count += 1
        self.edi_file_info.pop(0)
        self.__bulid_2010aa_loop()

    def __bulid_2010aa_loop(self):
        self.final_segment[self.segment]['2000A']['2010AA'] = {}
        for i in range(len(self.edi_file_info)):
            i = 0
            try:
                if self.edi_file_info[i].split('*')[0] == 'HL':
                    break
                else:
                    data_element = self.edi_file_info[i].split('*')
                    self.index += 1
                    self.count = 1
                    segment = data_element.pop(0) + '-' + str(self.index)
                    self.final_segment[self.segment]['2000A']['2010AA'][segment] = {}
                    for data in data_element:
                        data_element_count = '{:02}'.format(self.count)
                        self.final_segment[self.segment]['2000A']['2010AA'][segment][data_element_count] = data
                        self.count += 1
                    self.__pop_element(0)
            except IndexError:
                pass

    def __bulid_2000b_loop(self):
        self.final_segment[self.segment]['2000B'] = {}
        for i in range(len(self.edi_file_info)):
            i = 0
            try:
                if self.edi_file_info[i].split('*')[0] == 'NM1':
                    self.__bulid_2010ba_loop()
                    self.__bulid_2010bb_loop()
                    break
                else:
                    data_element = self.edi_file_info[i].split('*')
                    self.index += 1
                    self.count = 1
                    segment = data_element.pop(0) + '-' + str(self.index)
                    self.final_segment[self.segment]['2000B'][segment] = {}
                    for data in data_element:
                        data_element_count = '{:02}'.format(self.count)
                        self.final_segment[self.segment]['2000B'][segment][data_element_count] = data
                        self.count += 1
                    self.__pop_element(0)
            except IndexError:
                pass

    def __bulid_2010ba_loop(self):
        self.final_segment[self.segment]['2000B']['2010BA'] = {}
        data_element = self.edi_file_info[0].split('*')
        self.index += 1
        self.count = 1
        segment = data_element.pop(0) + '-' + str(self.index)
        self.final_segment[self.segment]['2000B']['2010BA'][segment] = {}
        for data in data_element:
            data_element_count = '{:02}'.format(self.count)
            self.final_segment[self.segment]['2000B']['2010BA'][segment][data_element_count] = data
            self.count += 1
        self.edi_file_info.pop(0)
        for i in range(len(self.edi_file_info)):
            i = 0
            try:
                if self.edi_file_info[i].split('*')[0] == 'NM1':
                    break
                else:
                    data_element = self.edi_file_info[0].split('*')
                    self.index += 1
                    self.count = 1
                    segment = data_element.pop(0) + '-' + str(self.index)
                    self.final_segment[self.segment]['2000B']['2010BA'][segment] = {}
                    for data in data_element:
                        data_element_count = '{:02}'.format(self.count)
                        self.final_segment[self.segment]['2000B']['2010BA'][segment][data_element_count] = data
                        self.count += 1
                    self.edi_file_info.pop(0)
            except IndexError:
                pass

    def __bulid_2010bb_loop(self):
        self.final_segment[self.segment]['2000B']['2010BB'] = {}
        for i in range(len(self.edi_file_info)):
            i = 0
            try:
                if self.edi_file_info[i].split('*')[0] == 'CLM':
                    break
                else:
                    data_element = self.edi_file_info[0].split('*')
                    self.index += 1
                    self.count = 1
                    segment = data_element.pop(0) + '-' + str(self.index)
                    self.final_segment[self.segment]['2000B']['2010BB'][segment] = {}
                    for data in data_element:
                        data_element_count = '{:02}'.format(self.count)
                        self.final_segment[self.segment]['2000B']['2010BB'][segment][data_element_count] = data
                        self.count += 1
                    self.edi_file_info.pop(0)
            except IndexError:
                pass

    def __bulid_2300_loop(self):
        self.final_segment[self.segment]['2300'] = {}
        for i in range(len(self.edi_file_info)):
            i = 0
            try:
                if self.edi_file_info[i].split('*')[0] == 'NM1':
                    self.__bulid_2310b_loop()
                    self.__bulid_2310c_loop()
                    self.__bulid_2400_loop()
                    break
                else:
                    data_element = self.edi_file_info[i].split('*')
                    self.index += 1
                    self.count = 1
                    segment = data_element.pop(0) + '-' + str(self.index)
                    self.final_segment[self.segment]['2300'][segment] = {}
                    for data in data_element:
                        data_element_count = '{:02}'.format(self.count)
                        self.final_segment[self.segment]['2300'][segment][data_element_count] = data
                        self.count += 1
                    self.__pop_element(0)
            except IndexError:
                pass

    def __bulid_2310b_loop(self):
        self.final_segment[self.segment]['2300']['2310B'] = {}
        data_element = self.edi_file_info[0].split('*')
        self.index += 1
        self.count = 1
        segment = data_element.pop(0) + '-' + str(self.index)
        self.final_segment[self.segment]['2300']['2310B'][segment] = {}
        for data in data_element:
            data_element_count = '{:02}'.format(self.count)
            self.final_segment[self.segment]['2300']['2310B'][segment][data_element_count] = data
            self.count += 1
        self.edi_file_info.pop(0)
        for i in range(len(self.edi_file_info)):
            i = 0
            try:
                if self.edi_file_info[i].split('*')[0] == 'NM1':
                    break
                else:
                    data_element = self.edi_file_info[0].split('*')
                    self.index += 1
                    self.count = 1
                    segment = data_element.pop(0) + '-' + str(self.index)
                    self.final_segment[self.segment]['2300']['2310B'][segment] = {}
                    for data in data_element:
                        data_element_count = '{:02}'.format(self.count)
                        self.final_segment[self.segment]['2300']['2310B'][segment][data_element_count] = data
                        self.count += 1
                    self.edi_file_info.pop(0)
            except IndexError:
                pass

    def __bulid_2310c_loop(self):
        self.final_segment[self.segment]['2300']['2310C'] = {}
        data_element = self.edi_file_info[0].split('*')
        self.index += 1
        self.count = 1
        segment = data_element.pop(0) + '-' + str(self.index)
        self.final_segment[self.segment]['2300']['2310C'][segment] = {}
        for data in data_element:
            data_element_count = '{:02}'.format(self.count)
            self.final_segment[self.segment]['2300']['2310C'][segment][data_element_count] = data
            self.count += 1
        self.edi_file_info.pop(0)
        for i in range(len(self.edi_file_info)):
            i = 0
            try:
                if self.edi_file_info[i].split('*')[0] == 'LX':
                    break
                else:
                    data_element = self.edi_file_info[0].split('*')
                    self.index += 1
                    self.count = 1
                    segment = data_element.pop(0) + '-' + str(self.index)
                    self.final_segment[self.segment]['2300']['2310C'][segment] = {}
                    for data in data_element:
                        data_element_count = '{:02}'.format(self.count)
                        self.final_segment[self.segment]['2300']['2310C'][segment][data_element_count] = data
                        self.count += 1
                    self.edi_file_info.pop(0)
            except IndexError:
                pass

    def __bulid_2400_loop(self):
        self.final_segment[self.segment]['2300']['2400'] = {}
        for i in range(len(self.edi_file_info)):
            i = 0
            try:
                if self.edi_file_info[i].split('*')[0] == 'ST' or self.edi_file_info[i].split('*')[0] == 'GE':
                    break
                else:
                    data_element = self.edi_file_info[i].split('*')
                    self.index += 1
                    self.count = 1
                    segment = data_element.pop(0) + '-' + str(self.index)
                    self.final_segment[self.segment]['2300']['2400'][segment] = {}
                    for data in data_element:
                        data_element_count = '{:02}'.format(self.count)
                        self.final_segment[self.segment]['2300']['2400'][segment][data_element_count] = data
                        self.count += 1
                    self.__pop_element(0)
            except IndexError:
                pass

    def __bulid_data_element(self, param):
        self.count = 1
        for self.data in self.data_element:
            data_element_count = '{:02}'.format(self.count)
            param[data_element_count] = self.data
            self.count += 1
        self.__pop_element(0)

    def create_837_index(self):
        for data in self.final_segment:
            segment = data.split('-')[0]
            if segment == 'ISA':
                self.info_837['837_index'][segment] = self.final_segment.get(data)
            if segment == 'GS':
                self.info_837['837_index'][segment] = self.final_segment.get(data)

            if segment == 'ST':
                self.info_837['837_index'][segment] = {}
                self.info_837['837_index'][segment]['01'] = self.final_segment.get(data).get('01')
                self.info_837['837_index'][segment]['02'] = self.final_segment.get(data).get('02')
                self.info_837['837_index'][segment]['count_st'] = self.count_st
                break

        self.connection.connect_837_index_collection()
        self.connection.insert_837_index_collection(self.info_837)
