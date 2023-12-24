"""This module is unused. It is only to show the way to break and map Patient object.
This approach can be applied to all the resource types but it is time comsuming"""
import logging
from fhir.resources.patient import Patient
import pandas as pd

logging.basicConfig(filename='transform_fhir.log', encoding='utf-8', level=logging.INFO)

def patient(pobj: Patient):
    """Transform fhir.resources.patient.Patient object to dataframe"""
    identifier_dict = {}
    for i in range(1, len(pobj.identifier)):
        if pobj.identifier[i].type.coding[0].code:
            identifier_dict[pobj.identifier[i].type.coding[0].code] = pobj.identifier[0].value
    
    telecom_dict = {}
    telecom_dict["system"] = pobj.telecom[0].system
    telecom_dict["use"] = pobj.telecom[0].use
    telecom_dict["value"] = pobj.telecom[0].value

    address_dict = {}
    address_dict["line"] = pobj.address[0].line[0]
    address_dict["city"] = pobj.address[0].city
    address_dict["state"] = pobj.address[0].state
    address_dict["country"] = pobj.address[0].country

    name_dict = {}
    name_dict["prefix"] = pobj.name[0].prefix[0]
    name_dict["given"] = pobj.name[0].given[0]
    name_dict["family"] = pobj.name[0].family
    patient_info = {
        "ID": pobj.id,
        "Name": name_dict,
        "Active": pobj.active,
        "Address": address_dict,
        "Gender": pobj.gender,
        "BirthDate": pobj.birthDate,
        "CommunicationType": pobj.communication[0].language.text,
        "Contact": pobj.contact,
        "Deceased": pobj.deceasedBoolean,
        "DeceasedDateTime": pobj.deceasedDateTime,
        "GeneralPractitioner": pobj.generalPractitioner,
        "Identifier": identifier_dict,
        "Link": pobj.link,
        "ManagingOrganization": pobj.managingOrganization,
        "MultipleBirthBool": pobj.multipleBirthBoolean,
        "MultipleBirthInt": pobj.multipleBirthInteger,
        "Photo": pobj.photo,
        "Telecom": telecom_dict
    }
    df = pd.DataFrame(patient_info)
    return df


