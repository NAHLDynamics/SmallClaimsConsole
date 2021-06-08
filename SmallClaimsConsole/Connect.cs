using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Tooling.Connector;
using System;
using System.Configuration;
using System.Net;
using System.ServiceModel;
using System.ServiceModel.Description;
using NLog;
using NLog.Internal;
using NAHL.D365.SQLLogger;

namespace SmallClaimsConsole
{
    class Connect
    {
        public static Logger localNLog;
        public static NAHLLogger LoggerSQL;

        public string connectionstring;
        public Connect(string _connectionstring, Logger logger, NAHLLogger loggerSQL)
        {
            localNLog = logger;
            LoggerSQL = loggerSQL;
            connectionstring = _connectionstring;
        }

        public IOrganizationService ConnectService()
        {

            IOrganizationService organizationService = null;

            var connection = System.Configuration.ConfigurationManager.ConnectionStrings[connectionstring].ConnectionString;
            CrmServiceClient serviceClient = new CrmServiceClient(connection);

            Console.WriteLine("Connected Org Friendly Name : " + serviceClient.ConnectedOrgFriendlyName);
            LoggerSQL.Info("Connected Org Friendly Name : " + serviceClient.ConnectedOrgFriendlyName);
            localNLog.Info("Connected Org Friendly Name : " + serviceClient.ConnectedOrgFriendlyName);
            localNLog.Info("CrmConnectOrgUriActual :" + serviceClient.CrmConnectOrgUriActual);
            localNLog.Info("Service Is Ready :  " + serviceClient.IsReady);
            localNLog.Info("Trying to initialise OrganizationServiceProxy");

            try
            {
                if (serviceClient.IsReady)
                {
                    //Logger.Info("Connection is ready : initiating Service Client");
                    ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
                    organizationService = (IOrganizationService)serviceClient.OrganizationWebProxyClient != null
                                        ? (IOrganizationService)serviceClient.OrganizationWebProxyClient
                                        : (IOrganizationService)serviceClient.OrganizationServiceProxy;

                    if (organizationService != null)
                    {
                        //Logger.Info("Seeking CRM Version");
                        RetrieveVersionRequest versionRequest = new RetrieveVersionRequest();
                        RetrieveVersionResponse versionResponse = (RetrieveVersionResponse)organizationService.Execute(versionRequest);
                        Console.WriteLine("Microsoft Dynamics CRM Version {0}", versionResponse.Version);
                        //Logger.Info("Found CRM Version " + versionResponse.Version);

                        // Test Call
                        Guid WhoAmIid = ((WhoAmIResponse)organizationService.Execute(new WhoAmIRequest())).UserId;
                        if (WhoAmIid != Guid.Empty)
                        {
                            localNLog.Info("Successful connection to CRM");
                            LoggerSQL.Info("Successful connection to CRM");
                            //Logger.Info("WhoAmI : " + WhoAmIid);
                            Entity user = organizationService.Retrieve("systemuser", WhoAmIid, new ColumnSet(true));
                            if (user != null)
                            {
                                localNLog.Info("UserName : " + user.GetAttributeValue<String>("fullname"));
                                //Logger.Info("DomainName : " + user.GetAttributeValue<String>("domainname"));
                            }
                            else
                            {
                                localNLog.Info("Unable to get user from CRM : WhoAmI request failed");
                                LoggerSQL.Info("Unable to get user from CRM : WhoAmI request failed");

                            }
                        }
                    }

                }
                else
                {
                    localNLog.Info("Last CRM Error : " + serviceClient.LastCrmError);
                    localNLog.Info("Last CRM Exception : " + serviceClient.LastCrmException);
                    localNLog.Info("Service was not ready for initialisation : IOrganizationService provision failed. Exiting");

                    LoggerSQL.Info("Last CRM Error : " + serviceClient.LastCrmError);
                    LoggerSQL.Info("Last CRM Exception : " + serviceClient.LastCrmException);
                    LoggerSQL.Info("Service was not ready for initialisation : IOrganizationService provision failed. Exiting");
                }

            }
            catch (FaultException<IOrganizationService> ex)
            {
                Console.WriteLine("Error : " + ex.Message);
                localNLog.Fatal(ex.Message);
                LoggerSQL.Fatal(ex.Message);
                throw;
            }
            catch (CommunicationException ex)
            {
                Console.WriteLine("Error : " + ex.Message);
                localNLog.Fatal(ex.Message);
                LoggerSQL.Fatal(ex.Message);
                throw;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error : " + ex.Message);
                localNLog.Fatal(ex.Message);
                LoggerSQL.Fatal(ex.Message);
                throw;
            }

            return organizationService;

        }
    }
}
