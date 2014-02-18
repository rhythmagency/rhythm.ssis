using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Web;
using Newtonsoft.Json.Linq;

namespace Rhythm.SSIS.Pipeline
{
    public class GeoCodingService
    {
        private const string GEO_CODE_BASE_URI = "http://maps.googleapis.com/maps/api/geocode/json";

        public GeoCoordinates GeoCodeAddress(string address)
        {
            var results = GetGeoCodeResults(address);

            return (from r in results
                    let geometry = r["geometry"]
                    where geometry != null
                    let location = geometry["location"]
                    where location != null
                    select new GeoCoordinates
                    {
                        Latitude = (decimal)location["lat"],
                        Longitude = (decimal)location["lng"]
                    }).FirstOrDefault();
        }

        public string GetZipCodeFromAddress(string address)
        {
            var results = GetGeoCodeResults(address);

            return (from r in results
                    let addressParts = r["address_components"]
                    where addressParts != null
                    from ap in addressParts
                    let types = ap["types"]
                    where types != null
                    let typesValues = types.Values<string>()
                    where typesValues.Contains("postal_code")
                    select (string)ap["short_name"]).FirstOrDefault();
        }

        private IEnumerable<JToken> GetGeoCodeResults(string address)
        {
            var client = new WebClient();

            string geoCodeUri = BuildGeoCodeUri(address);

            string jsonResponse = client.DownloadString(geoCodeUri);

            var response = JObject.Parse(jsonResponse);

            if (response != null)
            {
                return response["results"].ToArray();
            }

            return new JToken[0];
        }

        private string BuildGeoCodeUri(string address, bool sensor = false)
        {
            // example geocode request (google apis)
            // http://maps.googleapis.com/maps/api/geocode/json?address=1600+Amphitheatre+Parkway,+Mountain+View,+CA&sensor=false
            //
            // for additional documentation, see: https://developers.google.com/maps/documentation/geocoding/index#GeocodingRequests
            string lowercaseSensorFlag = sensor.ToString().ToLowerInvariant();
            return string.Format("{0}?address={1}&sensor={2}", GEO_CODE_BASE_URI, HttpUtility.UrlEncode(address), lowercaseSensorFlag);
        }
    }
}