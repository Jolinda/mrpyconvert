#
# Convenience functions for parsing Siemen's private tags
# without the need to track down the hex numbers to identify the tags
#
# Nicholas Stiffler
# Lewis Center for Neuroimaging
# July 15, 2018
#

import util_dicom_siemens as uds
import pickle
 
def getHeaderTag(dcm):
    for tag in dcm:
        if tag.VR == "LO" and "SIEMENS CSA HEADER" == tag.value:
            return tag.tag
    raise IndexError("Missing header tag")        

def parseMrProtocol(mp):
    mp_dict = {}

    if mp == "":
        return mp

    for line in mp.split("\n"):
        if line.startswith("##"):
            continue
       
        el = line.split("=")
        mp_dict[el[0].strip()] = el[1].strip(' "')
    return mp_dict

def parseMrPhoenixProtocol(mp):
    mp_dict = {}
    if mp == "":
        return mp

    mp_dict['XProtocol'] = ""
    
    parse = False 
    for line in mp.split("\n"):
        if line.startswith("###"):
            parse = not parse
            continue
        if not parse:
            mp_dict['XProtocol'] += line + "\n"
        else:
            el = line.split("=")
            if len(el) == 2:
                mp_dict[el[0].strip()] = el[1].strip('\t "')
    return mp_dict

def readCSAImageHeader(dcm):
    headerTag = getHeaderTag(dcm)
    element = headerTag.element * 0x100 + 0x10
    
    if (headerTag.group, element) in dcm:   
        return uds._parse_csa_header(dcm[headerTag.group, element].value)
    raise IndexError("Missing CSA Image Header")

def readCSASeriesHeader(dcm):
    headerTag = getHeaderTag(dcm)
    element = headerTag.element * 0x100 + 0x20

    if (headerTag.group, element) in dcm:
        seriesHeader = uds._parse_csa_header(dcm[headerTag.group, element].value)
        if "MrProtocol" in seriesHeader:
            seriesHeader['MrProtocol'] = parseMrProtocol(seriesHeader['MrProtocol'])
        if "MrPhoenixProtocol" in seriesHeader:
            seriesHeader['MrPhoenixProtocol'] = parseMrPhoenixProtocol(seriesHeader['MrPhoenixProtocol'])
        return seriesHeader
    raise IndexError("Missing CSA Series Header")

