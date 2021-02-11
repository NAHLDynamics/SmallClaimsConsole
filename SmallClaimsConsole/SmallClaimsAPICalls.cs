using Newtonsoft.Json.Linq;
using RestSharp;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Json;
using System.Text;
using System.Web;

namespace SmallClaimsConsole
{
    public class SmallClaimAPICalls
    {

        private string OAuthToken;
        public dynamic APIResponse;
        SmallClaimConnectorData connector;
        public SmallClaimAPICalls(string TokenURL, string ClientID, string ClientSecret, string Host, string Basepath, string jsonEndpoints)
        {

            try
            {
                connector = DeserializeJson<SmallClaimConnectorData>(jsonEndpoints);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            this.connector.TokenURL = TokenURL;
            this.connector.ClientID = ClientID;
            this.connector.ClientSecret = ClientSecret;
            this.connector.Host = Host;
            this.connector.Basepath = Basepath;
            this.connector.FlagEndpointListID();
        }

        private bool setOAuthToken()
        {
            try
            {
                String id = this.connector.ClientID;
                String secret = this.connector.ClientSecret;
                String tokenULR = this.connector.TokenURL;

                var client = new RestClient(tokenULR);
                var request = new RestRequest(Method.POST);
                request.AddHeader("cache-control", "no-cache");
                request.AddHeader("content-type", "application/x-www-form-urlencoded");
                request.AddParameter("application/x-www-form-urlencoded", "grant_type=client_credentials&scope=api&client_id=" + id + "&client_secret=" + secret, ParameterType.RequestBody);
                IRestResponse response = client.Execute(request);

                dynamic resp = JObject.Parse(response.Content);
                String token = resp.access_token;
                this.OAuthToken = token;
                return true;
            }
            catch (Exception ex)
            {
                string error = ex.Message;
                string errorerror = ex.Message;
                return false;
            }
        }

        public bool ServiceCall(string callTypeName, string callDataJson = null, params string[] paramlist)
        {

            SCEndpoint endpoint = this.connector.getEndpointByName(callTypeName);

            if (endpoint.Name == null)
                throw new Exception(string.Format("Unable to get Endpoint by name {0} . No parameters found!", callTypeName));

            if (!setOAuthToken())
                throw new Exception(string.Format("Unable to set OAuth token!"));

            try
            {

                RestClient client = new RestClient(this.connector.Host);

                string uri = "";
                if (endpoint.EndpointAddress.Contains("{"))
                {
                    uri = this.connector.Basepath + endpoint.EndpointAddress.Remove(endpoint.EndpointAddress.IndexOf('{')) + paramlist[1];
                }
                else
                {
                    uri = this.connector.Basepath + endpoint.EndpointAddress;
                }

                RestRequest request = new RestRequest(uri, restMethod(endpoint.Method));
                request.AddParameter("Authorization", $"Bearer {this.OAuthToken}", ParameterType.HttpHeader);
                request.AddHeader("Accept", "application/json");

                if (callDataJson != null)
                {
                    request.AddJsonBody(callDataJson, "application/json");
                }

                if (endpoint.HasIDParameter)
                {
                    if (paramlist.Count() == 0)
                    {
                        throw new Exception(string.Format("Endpoint {0} needs ID has one of its parameters. No parameters found!", endpoint.Name));
                    }
                }

                if (paramlist.Count() > 0 && !endpoint.EndpointAddress.Contains("{"))
                {
                    //for every two items in params list 
                    for (int p = 0; p < paramlist.Count(); p += 2)
                    {
                        request.AddParameter(paramlist[p], paramlist[p + 1]);
                    }
                }

                request.RequestFormat = DataFormat.Json;
                var response = client.Execute(request);

                if (response.Content != "")
                {
                    APIResponse = JObject.Parse(response.Content);
                    return true;
                }
                else
                {
                    throw new Exception(response.ErrorException.Message);
                }
            }
            catch (Exception ex)
            {
                string error = ex.Message;
                string errorerror = ex.Message;
                return false;
            }
        }

        private Method restMethod(string method)
        {

            switch (method)
            {
                case "POST":
                    return Method.POST;
                case "GET":
                    return Method.GET;
                /* No Method endpoint for these ATM
                case "PUT":
                    return Method.PUT;
                case "DELETE":
                    return Method.DELETE;
                case "PATCH":
                    return Method.PATCH;
                */
                default:
                    throw new Exception("Method Not found in ServiceCall.restMethod()!");
            }

        }

        public class SmallClaimConnectorData
        {

            public string TokenURL { get; set; }
            public string ClientID { get; set; }
            public string ClientSecret { get; set; }
            public string Host { get; set; }
            public string Basepath { get; set; }
            public EndpointsList EndpointsList { get; set; }

            internal void FlagEndpointListID()
            {
                foreach (SCEndpoint endpoint in EndpointsList.SCEndpoint)
                {
                    endpoint.HasIDParameter = endpoint.EndpointAddress.IndexOfAny(new char[] { '{', '}' }) > 0;
                }
            }

            internal SCEndpoint getEndpointByName(string callTypeName)
            {
                return EndpointsList.SCEndpoint.Where(e => e.Name == callTypeName).FirstOrDefault();
            }
        }
        public class EndpointsList
        {
            public IList<SCEndpoint> SCEndpoint { get; set; }
        }

        public class SCEndpoint
        {
            public string Name { get; set; }
            public string Method { get; set; }
            public string EndpointAddress { get; set; }
            public string Description { get; set; }
            public bool HasIDParameter { get; set; }
        }

        public T DeserializeJson<T>(string json)
        {
            var instance = Activator.CreateInstance<T>();
            using (var ms = new MemoryStream(Encoding.Unicode.GetBytes(json)))
            {
                var serializer = new DataContractJsonSerializer(instance.GetType());
                return (T)serializer.ReadObject(ms);
            }
        }

    }
}
