import sys
import os

import numpy as np
import pandas as pd

import matplotlib.pyplot as plt

from ooop import OOOP

config = {
    'ERP_HOSTNAME':'',
    'ERP_PORT': None,
    'ERP_NAME':'',
    'ERP_USER':'',
    'ERP_PASSWORD':''
}

DB_EFFIPEOPLE = 'data/effipeople_SomEnergia.csv'

class EffipeopleData(object):
    dbfile = None

    # climaticZone
    db_zc = {
        1:'Atlantica',
        2:'Mediterranea',
        3:'Continental'
    }

    # InsulationDetails_WindowFramesType
    db_windowFrameType = {
        1:'steel',
        2:'wood',
        3:'PVC'
    }

    # HomePlaceType
    db_homePlaceType = {
        1:'Single_house',
        2:'Apartment'
    }

    # HomeYear
    db_homeYear = {
        1: 1976, # 'pre_1977',
        2: 1999, # '1977_1999',
        3: 2000, # '2000_2006',
        4: 2006  # 'post_2006'
    }
    skip = ['Contract_CustomerId','Contract_Id']

    def __init__(self,dbfile):
        self.dbfile = dbfile
        self.df = pd.read_csv(self.dbfile,delimiter=';',header=0)
        self.df = self.df.drop(self.df.columns[0],1)
    def plot(self):
        features = self.df.columns
        for feature in features:
            plt.figure(figsize=(8,4))
            if np.issubdtype(self.df[feature].dtype, np.integer) and feature not in self.skip:
                self.df[feature].hist()
                plt.suptitle(feature)



class OpenERP(object):
    def __init__(self,config):
        self.O = None
        try:
            self.O = OOOP(dbname=config['ERP_NAME'],
                     user=config['ERP_USER'],
                     pwd=config['ERP_PASSWORD'],
                     port=config['ERP_PORT'],
                     uri=config['ERP_HOSTNAME'])
        except Exception, ex:
            raise ex


    def push_cups_building(self,cups_id,contract):

        params_write = {
            'buildingConstructionYear': EffipeopleData.db_homeYear.get(contract.PlaceDate,''),
            'buildingWindowsFrame': EffipeopleData.db_windowFrameType.get(contract.InsulationDetails_WindowFramesType,''),
            'dwellingArea': contract.SquareMeters,
            'buildingType': EffipeopleData.db_homePlaceType.get(contract.HomePlaceType,''),
        }

        if contract.InsulationDetails_IsWindowInsulationSimple:
            params_write.update({'buildingWindowsType':'single_panel'})

        if contract.ClimateControlEquipment_HeatingInstalled:
            if (contract.ClimateControlEquipment_IsHeatingElectric):
                params_write.update({'buildingHeatingSource':'electricity'})
            elif (contract.ClimateControlEquipment_IsHeatingCentralized):
                params_write.update({'buildingHeatingSource':'district_heating'})
        else:
            params_write.update({'buildingHeatingSource':'other'})

        if contract.Appliances_WaterHeater:
            params_write.update({'buildingHeatingSourceDhw':'electricity'})

        e_id = [self.O.EmpoweringCupsBuilding.search([('cups_id','=',cups_id)]) or None][0]
        if e_id:
            self.O.EmpoweringCupsBuilding.write(e_id, params_write)
        else:
            params_write.update({'cups_id': cups_id})
            self.O.EmpoweringCupsBuilding.create(params_write)


    def push_modcontractual(self,modcontractual_id,contract):
        params_write = {
            'totalPersonsNumber': contract.AdultsCount + contract.ChildrenCount,
            'minorPersonsNumber': contract.ChildrenCount
        }

        e_id = [self.O.EmpoweringModcontractualProfile.search([('modcontractual_id','=',modcontractual_id)]) or None][0]
        if e_id:
            self.O.EmpoweringModcontractualProfile.write(e_id, params_write)
        else:
            params_write.update({'modcontractual_id': modcontractual_id})
            self.O.EmpoweringModcontractualProfile.create(params_write)


    def push_contract(self,contract):
        params_search = [('name','=',str(contract.Contract_Id).zfill(5))]
        contract_id = [self.O.GiscedataPolissa.search(params_search,0,0,False,{'active_test': False}) or None][0]

        contract_read = self.O.GiscedataPolissa.read(contract_id,['cups','modcontractual_activa'])[0]

        cups_id = contract_read['cups'][0]
        modcontractual_id = contract_read['modcontractual_activa'][0]

        if not contract_id:
            return

        self.push_cups_building(cups_id,contract)
        self.push_modcontractual(modcontractual_id,contract)


openerp = OpenERP(config)

effidata =  EffipeopleData(DB_EFFIPEOPLE)
effidata.df.apply(lambda h: openerp.push_contract(h),axis=1)
