"""Script to update SGID StateFacilities with facility points from other sources."""
import arcpy
import os
import Levenshtein
from time import strftime

uniqueRunNum = strftime("%Y%m%d_%H%M%S")


class Feature (object):
    """Store usefull feature class information."""

    def __init__(self, workspace, name, spatialRef=None):
        """constructor."""
        self.workspace = workspace
        self.name = name
        self.path = os.path.join(workspace, name)
        self.ObjectIdField = arcpy.Describe(self.path).OIDFieldName
        self.spatialReference = spatialRef
        if self.spatialReference is None:
            self.spatialReference = arcpy.Describe(self.path).spatialReference

    @staticmethod
    def createFeature(workspace, name, spatialRef, geoType, fieldList=[]):
        """Create a feature class and retrun a feature object."""
        arcpy.CreateFeatureclass_management(workspace,
                                            name,
                                            geoType,
                                            spatial_reference=spatialRef)
        tempFeature = Feature(workspace, name, spatialRef)

        if len(fieldList) > 0:
            for field in fieldList:
                name = field[0]
                fieldType = field[1]
                if name == 'SHAPE@':
                    continue
                arcpy.AddField_management(tempFeature.path,
                                          name,
                                          fieldType)

        return tempFeature

    @staticmethod
    def createFeatureFromLayer(workspace, name, layer):
        """Create a feature class and retrun a feature object."""
        tempFeature = Feature(workspace, name, arcpy.Describe(layer).spatialReference)
        arcpy.CopyFeatures_management(layer, os.path.join(workspace, name))

        return tempFeature


def matchedIdDistances(sgidPoints, otherPoints):
    """Get distances from points in each dataset that have matching building ID"""
    newBuildingsById = {}
    newBuildingsByName = {}
    newUnmatchedRowList = []

    with arcpy.da.SearchCursor(otherPoints.path,
                               ['AssetID', 'Asset_Name', 'SHAPE@', 'OID@']) as cursor:
        for row in cursor:
            assetId = row[0]
            if assetId is not None and assetId != '' and assetId not in newBuildingsById:
                newBuildingsById[assetId] = row
            else:
                newUnmatchedRowList.append(row)

    idCounter = 0
    nameCounter = 0
    sgidUnmatchedRowList = []
    matchLineFields = (
        ('oldObjId', 'LONG'),
        ('newObjId', 'LONG'),
        ('oldBuildId', 'LONG'),
        ('newBuildId', 'LONG'),
        ('oldName', 'TEXT'),
        ('newName', 'TEXT'),
        ('nameLDist', 'LONG'),
        ('matchType', 'TEXT'),
        ('SHAPE@', 'GEOMETERY')
    )
    matchLines = Feature.createFeature(outputWorkspace,
                                       'matchLines_' + uniqueRunNum,
                                       sgidPoints.spatialReference,
                                       'POLYLINE',
                                       matchLineFields)
    with arcpy.da.InsertCursor(matchLines.path, [x[0] for x in matchLineFields]) as matchCursor:
        with arcpy.da.SearchCursor(sgidPoints.path,
                                   ['BuildingId', 'BUILDNAME', 'SHAPE@', 'OID@']) as cursor:
            for row in cursor:
                oldId, oldName, oldPoint, oldObjId = row

                if oldId in newBuildingsById:
                    newFac = newBuildingsById[oldId]
                    newId, newName, newPoint, newObjId = newFac
                    nameLDist = None
                    if oldName is not None and newName is not None:
                        nameLDist = Levenshtein.distance(oldName, newName)
                    idCounter += 1
                    matchCursor.insertRow((
                        oldObjId,
                        newObjId,
                        oldId,
                        newId,
                        oldName,
                        newName,
                        nameLDist,
                        'ID',
                        arcpy.Polyline(
                            arcpy.Array([oldPoint.centroid, newPoint.centroid]),
                            matchLines.spatialReference
                            )
                    ))
                    del newBuildingsById[oldId]
                else:
                    sgidUnmatchedRowList.append(row)

        newUnmatchedRowList.extend(newBuildingsById.values())

    print idCounter


def updatePositions(basePoints, baseIdField, newPoints, newIdField):
    """Update points position based on shared id in new positions."""
    newBuildingsById = {}
    updatedOids = []
    with arcpy.da.SearchCursor(newPoints.path,
                               ['AssetID', 'Asset_Name', 'SHAPE@', 'OID@']) as cursor:
        for row in cursor:
            assetId = row[0]
            if assetId is not None and assetId != '':
                idRows = newBuildingsById.get(assetId, [])
                idRows.append(row)
                newBuildingsById[assetId] = idRows
            # else:
            #     print 'Duplicate newPoint ID: {}'.format(assetId)

    oldUpdateRowsById = {}
    oldFields = [baseIdField, 'SHAPE@', 'OID@']
    oldFields.extend([f.name for f in arcpy.ListFields(basePoints.path) if not f.name.lower().startswith('shape') and f.name != 'OBJECTID'])
    with arcpy.da.UpdateCursor(sgidPoints.path,
                               oldFields) as cursor:
        for row in cursor:
            oldId = row[0]
            oldObjId = row[2]

            if oldId in newBuildingsById:
                newFacList = newBuildingsById[oldId]
                newFac = newFacList[0]
                newId, newName, newPoint, newObjId = newFac
                row[1] = newPoint
                cursor.updateRow(row)
                updatedOids.append(oldObjId)
                newFacList.pop(0)
                print
                if len(newFacList) == 0:
                    del newBuildingsById[oldId]
                else:
                    oldUpdateRowsById[oldId] = row

    with arcpy.da.InsertCursor(sgidPoints.path,
                               oldFields) as cursor:
        for newId in newBuildingsById:
            if newId in oldUpdateRowsById:
                for newRow in newBuildingsById[newId]:
                    newId, newName, newPoint, newObjId = newRow
                    oldRow = oldUpdateRowsById[newId]
                    oldRow[1] = newPoint
                    insOid = cursor.insertRow(oldRow)
                    updatedOids.append(insOid)
                print

    return updatedOids


if __name__ == '__main__':
    global outputWorkspace
    outputWorkspace = r'C:\GisWork\StateFacilities\outputs.gdb'
    dataWorkspace = r'C:\GisWork\StateFacilities\data.gdb'
    baseSgidPoints = Feature(r'C:\GisWork\StateFacilities\StateFacilities.gdb',
                             'SGID_StateFacilities_base')
    arcpy.CopyFeatures_management(baseSgidPoints.path,
                                  os.path.join(dataWorkspace, 'SGID_StateFacilities_' + uniqueRunNum))
    sgidPoints = Feature(dataWorkspace,
                         'SGID_StateFacilities_' + uniqueRunNum)
    newPoints = Feature(r'C:\GisWork\StateFacilities\StateFacilities.gdb',
                        'Building_Points')
    # matchedIdDistances(sgidPoints, newPoints)
    updatedOids = updatePositions(sgidPoints, 'BuildingId', newPoints, 'AssetID')
    print len(updatedOids)
