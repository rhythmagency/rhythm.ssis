using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.IO;
using System.Linq;
using Microsoft.SqlServer.Dts.Pipeline;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;
using Microsoft.SqlServer.Types;


namespace Rhythm.SSIS.Pipeline
{
    [DtsPipelineComponent(
        DisplayName = "GeoCode Address",
        Description = "Transform an address to Latitude/Longitude using Google APIs",
        IconResource = "Rhythm.SSIS.Pipeline.Resources.GeoCodeIcon.ico")]
    public class GoogleGeoCodePipelineComponent : PipelineComponent {
        
        const string ErrorInvalidUsageType = "Invalid UsageType for column '{0}'";
        const string ErrorInvalidDataType = "Invalid DataType for column '{0}'";

        public string FullAddressColumnName {
            get { return GetPropertyStringValue("FullAddressColumnName"); }
        }
        
        public string Address1ColumnName
        {
            get { return GetPropertyStringValue("Address1ColumnName"); }
        }
        
        public string Address2ColumnName
        {
            get { return GetPropertyStringValue("Address2ColumnName"); }
        }
        
        public string CityColumnName
        {
            get { return GetPropertyStringValue("CityColumnName"); }
        }
        
        public string StateColumnName
        {
            get { return GetPropertyStringValue("StateColumnName"); }
        }
        
        public string ZipColumnName
        {
            get { return GetPropertyStringValue("ZipColumnName"); }
        }
        
        public string LatitudeColumnName
        {
            get { return GetPropertyStringValue("LatitudeColumnName"); }
        }
        
        public string LongitudeColumnName
        {
            get { return GetPropertyStringValue("LongitudeColumnName"); }
        }
        
        public string LocationColumnName
        {
            get { return GetPropertyStringValue("LocationColumnName"); }
        }

        private Dictionary<string, int> _columnIndices = new Dictionary<string, int>();
        private Dictionary<string, int> _propertyIndices = new Dictionary<string, int>();

        private MemoryStream _tmpMemoryStream;
        private BinaryWriter _tmpBinaryWriter;

        private string GetPropertyStringValue(string propertyName)
        {
            if (_propertyIndices.ContainsKey(propertyName))
            {
                return (string)ComponentMetaData.CustomPropertyCollection[_propertyIndices[propertyName]].Value;
            }

            return null;
        }

        public override void ProvideComponentProperties() {

            ComponentMetaData.RuntimeConnectionCollection.RemoveAll();
            RemoveAllInputsOutputsAndCustomProperties();

            ComponentMetaData.UsesDispositions = true;

            // Add input
            var input = ComponentMetaData.InputCollection.New();
            input.Name = "Input";

            input.ErrorRowDisposition = DTSRowDisposition.RD_IgnoreFailure;

            // Add output
            var output = ComponentMetaData.OutputCollection.New();
            output.Name = "Output";
            output.SynchronousInputID = input.ID;
            output.ExclusionGroup = 1;

            AddErrorOutput("Errors", input.ID, output.ExclusionGroup);

            var fullAddressColumnName = ComponentMetaData.CustomPropertyCollection.New();
            fullAddressColumnName.Name = "FullAddressColumnName";
            fullAddressColumnName.Value = "Address";
            _propertyIndices.Add(fullAddressColumnName.Name, ComponentMetaData.CustomPropertyCollection.FindObjectIndexByID(fullAddressColumnName.ID));

            var address1ColumnName = ComponentMetaData.CustomPropertyCollection.New();
            address1ColumnName.Name = "Address1ColumnName";
            address1ColumnName.Value = "Address1";
            _propertyIndices.Add(address1ColumnName.Name, ComponentMetaData.CustomPropertyCollection.FindObjectIndexByID(address1ColumnName.ID));

            var address2ColumnName = ComponentMetaData.CustomPropertyCollection.New();
            address2ColumnName.Name = "Address2ColumnName";
            address2ColumnName.Value = "Address2";
            _propertyIndices.Add(address2ColumnName.Name, ComponentMetaData.CustomPropertyCollection.FindObjectIndexByID(address2ColumnName.ID));

            var cityColumnName = ComponentMetaData.CustomPropertyCollection.New();
            cityColumnName.Name = "CityColumnName";
            cityColumnName.Value = "City";
            _propertyIndices.Add(cityColumnName.Name, ComponentMetaData.CustomPropertyCollection.FindObjectIndexByID(cityColumnName.ID));

            var stateColumnName = ComponentMetaData.CustomPropertyCollection.New();
            stateColumnName.Name = "StateColumnName";
            stateColumnName.Value = "State";
            _propertyIndices.Add(stateColumnName.Name, ComponentMetaData.CustomPropertyCollection.FindObjectIndexByID(stateColumnName.ID));

            var zipColumnName = ComponentMetaData.CustomPropertyCollection.New();
            zipColumnName.Name = "ZipColumnName";
            zipColumnName.Value = "Zip";
            _propertyIndices.Add(zipColumnName.Name, ComponentMetaData.CustomPropertyCollection.FindObjectIndexByID(zipColumnName.ID));

            var latitudeColumnName = ComponentMetaData.CustomPropertyCollection.New();
            latitudeColumnName.Name = "LatitudeColumnName";
            latitudeColumnName.Value = "Latitude";
            _propertyIndices.Add(latitudeColumnName.Name, ComponentMetaData.CustomPropertyCollection.FindObjectIndexByID(latitudeColumnName.ID));

            var longitudeColumnName = ComponentMetaData.CustomPropertyCollection.New();
            longitudeColumnName.Name = "LongitudeColumnName";
            longitudeColumnName.Value = "Longitude";
            _propertyIndices.Add(longitudeColumnName.Name, ComponentMetaData.CustomPropertyCollection.FindObjectIndexByID(longitudeColumnName.ID));

            var locationColumnName = ComponentMetaData.CustomPropertyCollection.New();
            locationColumnName.Name = "LocationColumnName";
            locationColumnName.Value = "Location";
            _propertyIndices.Add(locationColumnName.Name, ComponentMetaData.CustomPropertyCollection.FindObjectIndexByID(locationColumnName.ID));
        }
        
        public override void ReinitializeMetaData()
        {
            var input = ComponentMetaData.InputCollection[0];

            if (!ComponentMetaData.AreInputColumnsValid)
                ComponentMetaData.RemoveInvalidInputColumns();

            base.ReinitializeMetaData();
        }

        [CLSCompliant(false)]
        public override DTSValidationStatus Validate()
        {
            bool Cancel;
            if (ComponentMetaData.AreInputColumnsValid == false)
                return DTSValidationStatus.VS_NEEDSNEWMETADATA;

            var inputColumns = ComponentMetaData.InputCollection[0].InputColumnCollection.Cast<IDTSInputColumn100>().ToArray();

            var usingFullAddressColumn = inputColumns.Any(c => c.Name.Equals(FullAddressColumnName));

            if (!usingFullAddressColumn)
            {
                // expect address1, address2, city, state, zip
                var selectedColumns = inputColumns.Select(c => c.Name);
                var expectedColumns = new[]{Address1ColumnName, Address2ColumnName, CityColumnName, StateColumnName, ZipColumnName};

                var neededColumns = expectedColumns.Except(selectedColumns).Where(n => !string.IsNullOrWhiteSpace(n)).ToArray();

                if (neededColumns.Length > 0)
                {
                    ComponentMetaData.FireError(0, GetType().Name, String.Format("Columns required, but not found: {0}", string.Join(",", neededColumns)), "", 0, out Cancel);
                    return DTSValidationStatus.VS_ISBROKEN;
                }
            }

            var usingLocationColumn = inputColumns.Any(c => c.Name.Equals(LocationColumnName));

            if (!usingLocationColumn)
            {
                var selectedColumns = inputColumns.Select(c => c.Name);
                var expectedColumns = new[] { LatitudeColumnName, LongitudeColumnName };

                var neededColumns = expectedColumns.Except(selectedColumns).Where(n => !string.IsNullOrWhiteSpace(n)).ToArray();

                if (neededColumns.Length > 0)
                {
                    ComponentMetaData.FireError(0, GetType().Name, String.Format("Columns required, but not found: {0}", string.Join(",", neededColumns)), "", 0, out Cancel);
                    return DTSValidationStatus.VS_ISBROKEN;
                }
            }

            foreach (var inputColumn in inputColumns)
            {
                if (!IsExpectedUsageType(inputColumn.Name, inputColumn.UsageType))
                {
                    ComponentMetaData.FireError(0, inputColumn.IdentificationString, String.Format(ErrorInvalidUsageType, inputColumn.Name), "", 0, out Cancel);
                    return DTSValidationStatus.VS_ISBROKEN;
                }

                if (!IsExpectedDataType(inputColumn.Name, inputColumn.DataType))
                {
                    ComponentMetaData.FireError(0, inputColumn.IdentificationString, String.Format(ErrorInvalidUsageType, inputColumn.Name), "", 0, out Cancel);
                    return DTSValidationStatus.VS_ISBROKEN;
                }
            }
            return base.Validate();
        }

        [CLSCompliant(false)]
        public override IDTSInputColumn100 SetUsageType(int inputID, IDTSVirtualInput100 virtualInput, int lineageID, DTSUsageType usageType)
        {
            IDTSVirtualInputColumn100 virtualInputColumn = virtualInput.VirtualInputColumnCollection.GetVirtualInputColumnByLineageID(lineageID);
            if (!IsExpectedUsageType(virtualInputColumn.Name, virtualInputColumn.UsageType))
            {
                throw new Exception(String.Format(ErrorInvalidUsageType, virtualInputColumn.Name));
            }

            if (!IsExpectedDataType(virtualInputColumn.Name, virtualInputColumn.DataType))
            {
                throw new Exception(String.Format(ErrorInvalidDataType, virtualInputColumn.Name));
            }

            return base.SetUsageType(inputID, virtualInput, lineageID, usageType);
        }

        [CLSCompliant(false)]
        public override IDTSOutput100 InsertOutput(DTSInsertPlacement insertPlacement, int outputID)
        {
            throw new Exception("You cannot insert an output (" + outputID.ToString() + ")");
        }

        [CLSCompliant(false)]
        public override IDTSInput100 InsertInput(DTSInsertPlacement insertPlacement, int inputID)
        {
            throw new Exception("You cannot insert an input  (" + inputID.ToString() + ")");
        }

        [CLSCompliant(false)]
        public override void DeleteInput(int inputID)
        {
            throw new Exception("You cannot delete an input");
        }

        [CLSCompliant(false)]
        public override void DeleteOutput(int outputID)
        {
            throw new Exception("You cannot delete an ouput");
        }
        
        public override void PreExecute() {
            IDTSInput100 input = ComponentMetaData.InputCollection[0];
            _columnIndices.Clear();

            var inputColumns = input.InputColumnCollection;

            foreach (IDTSInputColumn100 inputColumn in inputColumns)
            {
                var columnIndex = BufferManager.FindColumnByLineageID(input.Buffer, inputColumn.LineageID);
                _columnIndices.Add(inputColumn.Name, columnIndex);
            }

            foreach (var property in ComponentMetaData.CustomPropertyCollection.Cast<IDTSCustomProperty100>())
            {
                var propertyIndex = ComponentMetaData.CustomPropertyCollection.FindObjectIndexByID(property.ID);
                _propertyIndices.Add(property.Name, propertyIndex);
            }

            _tmpMemoryStream = new MemoryStream(10000);
            _tmpBinaryWriter = new BinaryWriter(_tmpMemoryStream);
        }
        
        public override void ProcessInput(int inputID, PipelineBuffer buffer) {
            var geoCodingService = new GeoCodingService();

            var output = ComponentMetaData.OutputCollection["Output"];

            while (buffer.NextRow())
            {
                bool modified = false;
                string address = GetFullAddressFromBuffer(buffer);

                if (!string.IsNullOrWhiteSpace(address))
                {
                    var coordinates = geoCodingService.GeoCodeAddress(address);
                    if (coordinates != null)
                    {
                        if (_columnIndices.ContainsKey(LatitudeColumnName))
                        {
                            buffer.SetDecimal(_columnIndices[LatitudeColumnName], coordinates.Latitude);
                            modified = true;
                        }

                        if (_columnIndices.ContainsKey(LongitudeColumnName))
                        {
                            buffer.SetDecimal(_columnIndices[LongitudeColumnName], coordinates.Longitude);
                            modified = true;
                        }

                        if (_columnIndices.ContainsKey(LocationColumnName))
                        {
                            var pointTaggedText = new SqlChars(string.Format("POINT({0} {1})", coordinates.Longitude, coordinates.Latitude));
	                        var sqlGeography = SqlGeography.STPointFromText(pointTaggedText, 4326);
                            
                            _tmpMemoryStream.SetLength(0);
                            sqlGeography.Write(_tmpBinaryWriter);
                            _tmpBinaryWriter.Flush();

                            buffer.AddBlobData(_columnIndices[LocationColumnName], _tmpMemoryStream.GetBuffer(), (int)_tmpMemoryStream.Length);
                            modified = true;
                        }
                    }
                }

                if (modified)
                {
                    // send buffered row to output
                    buffer.DirectRow(output.ID);
                }
            }
        }

        private bool IsExpectedDataType(string columnName, DataType dataType)
        {
            if (columnName.Equals(FullAddressColumnName)
                || columnName.Equals(Address1ColumnName)
                || columnName.Equals(Address2ColumnName)
                || columnName.Equals(CityColumnName)
                || columnName.Equals(StateColumnName)
                || columnName.Equals(ZipColumnName))
            {
                return dataType == DataType.DT_STR || dataType == DataType.DT_WSTR;
            }

            if (columnName.Equals(LatitudeColumnName) || columnName.Equals(LongitudeColumnName))
            {
                return dataType == DataType.DT_NUMERIC;
            }

            if (columnName.Equals(LocationColumnName))
            {
                return dataType == DataType.DT_IMAGE;
            }

            return true;
        }

        private bool IsExpectedUsageType(string columnName, DTSUsageType usageType)
        {
            //if (columnName.Equals(FullAddressColumnName)
            //    || columnName.Equals(Address1ColumnName)
            //    || columnName.Equals(Address2ColumnName)
            //    || columnName.Equals(CityColumnName)
            //    || columnName.Equals(StateColumnName)
            //    || columnName.Equals(ZipColumnName))
            //{
            //    return usageType == DTSUsageType.UT_READONLY;
            //}

            //if (columnName.Equals(LatitudeColumnName)
            //    || columnName.Equals(LongitudeColumnName)
            //    || columnName.Equals(LocationColumnName))
            //{
            //    return usageType == DTSUsageType.UT_READWRITE;
            //}

            return true;
        }

        private string GetFullAddressFromBuffer(PipelineBuffer buffer) {
            if (_columnIndices.ContainsKey(FullAddressColumnName))
            {
                var fullAddress = buffer.GetString(_columnIndices[FullAddressColumnName]);
                if (!string.IsNullOrWhiteSpace(fullAddress))
                {
                    return fullAddress;
                }
            }
            else
            {
                var addressParts = new List<string>();

                if (_columnIndices.ContainsKey(Address1ColumnName))
                {
                    var address1 = buffer.GetString(_columnIndices[Address1ColumnName]);
                    if (!string.IsNullOrWhiteSpace(address1))
                    {
                        addressParts.Add(address1);
                    }
                }

                if (_columnIndices.ContainsKey(Address2ColumnName))
                {
                    var address2 = buffer.GetString(_columnIndices[Address2ColumnName]);
                    if (!string.IsNullOrWhiteSpace(address2))
                    {
                        addressParts.Add(address2);
                    }
                }

                if (_columnIndices.ContainsKey(CityColumnName))
                {
                    var city = buffer.GetString(_columnIndices[CityColumnName]);
                    if (!string.IsNullOrWhiteSpace(city))
                    {
                        addressParts.Add(city);
                    }
                }

                if (_columnIndices.ContainsKey(StateColumnName))
                {
                    var state = buffer.GetString(_columnIndices[StateColumnName]);
                    if (!string.IsNullOrWhiteSpace(state))
                    {
                        addressParts.Add(state);
                    }
                }

                if (_columnIndices.ContainsKey(ZipColumnName))
                {
                    var zip = buffer.GetString(_columnIndices[ZipColumnName]);
                    if (!string.IsNullOrWhiteSpace(zip))
                    {
                        addressParts.Add(zip);
                    }
                }

                var result = string.Join(", ", addressParts);

                if (!string.IsNullOrWhiteSpace(result))
                {
                    return result;
                }
            }

            return null;
        }
    }
}
