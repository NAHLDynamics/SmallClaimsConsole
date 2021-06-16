using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using NAHL.D365.SQLLogger;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NLog;

namespace SmallClaimsConsole
{
    class Program
    {
        public static Logger localNLog = LogManager.GetLogger("localLog");
        public static NAHLLogger LoggerSQL = new NAHLLogger("Small Claims Console", "Once every hour", true, true, true, localNLog, 1);

        static IOrganizationService service;
        static DynamicHelpers helpers;

        //Testing vars : 
        /*FOR TESTING UPDATE isTesting VALUE IN APP.CONFIG TO TRUE*/
        static bool IsTesting = bool.Parse(ConfigurationManager.AppSettings.Get("isTesting"));
        static bool ContinueApplication = true;
        static bool OverrideGetUpdates = false;
        static string connectOrg;

        static string overrideClaimID;
        static string dateFrom;// = "20191001T000000.000 GMT"; // string formattedNow = now.ToString("yyyy-MM-ddTHH:mm:ss.fffzzz");
        static string timeFrom;// = "10:11:12"; //HH:MM:SS
        static string dateTo;// = "20211001T000000.000 GMT"; // string formattedNow = now.ToString("yyyy-MM-ddTHH:mm:ss.fffzzz");

        static void Main(string[] args)
        {

            #region Console Testing Aid

            /*If testing allow user to input org to connect to*/
            if (IsTesting)
            {
                //This represents the NOW time that the Console App is running
                System.Console.WriteLine("Please type CRM Dynamics ORG to connect to (Orgs available : DEV - SND - UAT)");

                string org = Console.ReadLine();

                if (new string[]{ "dev", "snd", "uat" }.Contains(org.ToLower()))
                {
                    Console.WriteLine("CRM Dynamics Org {0} selected, press any key to continue, or Escape to exit.", org);
                    string keypressed = Console.ReadKey().Key.ToString();
                    if (keypressed == "Escape")
                    {
                        ContinueApplication = false;
                    }
                    else 
                    { 
                        connectOrg = "nahl" + org.ToLower();
                        localNLog.Info("Running application on Org : " + org);
                    }
                }
                else
                {
                    Console.WriteLine("No org selected, defaulting to DEV. Press any key to continue or ESC to exit.");
                    string keypressed = Console.ReadKey().Key.ToString();
                    if (keypressed == "Escape")
                    {
                        ContinueApplication = false;
                    }
                    else
                    {
                        connectOrg = "nahldev";
                        localNLog.Info("Running application on Org : " + org);
                    }
                }

                System.Console.WriteLine("Would you like to define a Claim ID? Y/N (Y= Use Claim ID / N= Use Date range)");
                string useclaimid = Console.ReadLine();

                if (useclaimid.ToLower() == "y")
                {
                    System.Console.WriteLine("Please type Claim ID - It can be found on the Small Claim Information form (example of Claim Id : OIC-00001) ");
                    string overrideclaimid = Console.ReadLine();

                    try
                    {
                        if(overrideclaimid.Contains("OIC") || overrideclaimid.Contains("CL")) { 
                            overrideClaimID = overrideclaimid;
                            OverrideGetUpdates = true;
                            System.Console.WriteLine("Override Claim ID set, continuing with program.");
                        }
                        else
                        {
                            System.Console.WriteLine("Claim ID not set, please check the format.");
                            ContinueApplication = false;
                        }
                    }
                    catch (Exception ex)
                    {
                        System.Console.WriteLine("Claim ID not returned, program will terminate. Error : " + ex.Message);
                        ContinueApplication = false;
                    }
                } else { 

                    System.Console.WriteLine("Would you like to define the Date From? Y/N ");
                    string usedate = Console.ReadLine();

                    if(usedate.ToLower() == "y")
                    {
                        System.Console.WriteLine("Please type date : Format : DD-MM-YYYY HH:MM:SS");
                        string date = Console.ReadLine();

                        try 
                        {
                            dateFrom = DateTime.Parse(date).ToString("yyyyMMdd'T'HHmmss.fff") + " GMT";
                            timeFrom = DateTime.Parse(date).ToString("HH:mm:ss"); //HH:MM:SS - 10:11:12
                            dateTo = DateTime.Now.ToString("yyyyMMdd'T'HHmmss.fff") + " GMT";//date format expected example : "20191001T000000.000 GMT"
                        }
                        catch (Exception ex)
                        {
                            System.Console.WriteLine("Date not recognised, program will terminate. Error : " + ex.Message);
                            ContinueApplication = false;
                        }
                    }
                }
            }
            else
            {
                connectOrg = "nallive";
            }

            #endregion
            
            if (ContinueApplication) 
            { 
                
                Connect connection = new Connect(connectOrg, localNLog, LoggerSQL);
                service = connection.ConnectService();

                if (service != null)
                {
                    try
                    {

                        helpers = new DynamicHelpers(localNLog, service);

                        #region Zettings Payload
                        var ZettingsRecordId = ConfigurationManager.AppSettings["ZettingsRecordId"];
                        if (ZettingsRecordId == null)
                        {
                            ZettingsRecordId = "1A9A3475-D511-4A1F-908F-6D71452CA9CF";// DEV DEFAULT
                        }

                        //Retrive API Settings From Dynamics
                        Entity Zettings = helpers.generalQuery(
                            "nal_zettings",
                            new ColumnSet("nal_payload"),
                            new List<ConditionExpression> { new ConditionExpression("nal_specificidentitykey", ConditionOperator.Equal, ZettingsRecordId) },
                            new OrderExpression("createdon", OrderType.Descending),
                            true
                        ).Entities.FirstOrDefault();

                        if (Zettings != null)
                        {
                            if (!Zettings.Contains("nal_payload"))
                            {
                                Console.WriteLine("Zettings Error : Zettings settings missing crucial payload information");
                                localNLog.Fatal("Zettings Error : Zettings settings missing crucial payload information");
                                LoggerSQL.Fatal("Zettings Error : Zettings settings missing crucial payload information");
                            }
                        }
                        else
                        {
                            Console.WriteLine("Zettings Error : Failed to retrive Zettings information");
                            localNLog.Fatal("Zettings Error : Failed to retrive Zettings information");
                            LoggerSQL.Fatal("Zettings Error : Failed to retrive Zettings information");
                        }

                        string endpointsJson = Zettings.GetAttributeValue<string>("nal_payload");
                        #endregion

                        #region API Settings Info
                        var APISettingsRecordId = ConfigurationManager.AppSettings["APISettingsRecordId"];
                        if (APISettingsRecordId == null)
                        {
                            APISettingsRecordId = "4F63E58E-8655-EB11-8128-005056B21276";// DEV DEFAULT
                        }

                        //Retrive API Settings From Dynamics
                        Entity APISettings = helpers.generalQuery(
                            "nal_apisettings",
                            new ColumnSet(true),
                            new List<ConditionExpression> { new ConditionExpression("nal_apisettingsid", ConditionOperator.Equal, APISettingsRecordId) },
                            new OrderExpression("createdon", OrderType.Descending),
                            true
                        ).Entities.FirstOrDefault();

                        if (APISettings != null)
                        {
                            if (!APISettings.Contains("nal_certificate") || !APISettings.Contains("nal_username") || !APISettings.Contains("nal_password") || !APISettings.Contains("nal_servicehost") || !APISettings.Contains("nal_servicebasepath"))
                            {
                                Console.WriteLine("API Error : API settings missing crucial information");
                                localNLog.Fatal("API Error : API settings missing crucial information");
                                LoggerSQL.Fatal("API Error : API settings missing crucial information");
                            }
                        }
                        else
                        {
                            Console.WriteLine("API Error : Failed to retrive API settings");
                            localNLog.Fatal("API Error : Failed to retrive API settings");
                            LoggerSQL.Fatal("API Error : Failed to retrive API settings");
                        }

                        SmallClaimAPICalls smallClaimCalls = new SmallClaimAPICalls(
                            APISettings.GetAttributeValue<string>("nal_certificate"),
                            APISettings.GetAttributeValue<string>("nal_username"),
                            APISettings.GetAttributeValue<string>("nal_password"),
                            APISettings.GetAttributeValue<string>("nal_servicehost"),
                            APISettings.GetAttributeValue<string>("nal_servicebasepath"),
                            endpointsJson
                        );
                        #endregion

                        if (!OverrideGetUpdates) 
                        {
                            #region Get Run DateTime for Assignment and Notification 

                            if (dateFrom == null)
                            {
                                try
                                {

                                    EntityCollection lastCreatedHistory = helpers.generalQuery("nal_smallclaimshistory", new ColumnSet("createdon"),
                                        new List<ConditionExpression>()
                                        {
                                        new ConditionExpression("nal_ingestedfromportal", ConditionOperator.Equal, true),
                                        }
                                        , new OrderExpression("createdon", OrderType.Descending)
                                        , true);
                                    DateTime lastran = lastCreatedHistory.Entities[0].GetAttributeValue<DateTime>("createdon");

                                    dateFrom = lastran.ToString("yyyyMMdd'T'HHmmss.fff") + " GMT"; //date format expected example : "20191001T000000.000 GMT"
                                    timeFrom = lastran.ToString("HH:mm:ss"); //HH:MM:SS - 10:11:12
                                    dateTo = DateTime.Now.ToString("yyyyMMdd'T'HHmmss.fff") + " GMT";//date format expected example : "20191001T000000.000 GMT"
                                }
                                catch (Exception ex)
                                {
                                    dateFrom = DateTime.Now.AddDays(-1).ToString("yyyyMMdd'T'HHmmss.fff") + " GMT"; //date format expected example : "20191001T000000.000 GMT"
                                    timeFrom = DateTime.Now.ToString("HH:MM:SS"); //HH:MM:SS - 10:11:12
                                    dateTo = DateTime.Now.ToString("yyyyMMdd'T'HHmmss.fff") + " GMT";//date format expected example : "20191001T000000.000 GMT"

                                    Console.WriteLine("Failed to find last created datetime, using following : \ndateFrom {0}\ntimeFrom {0}\ndateTo {0}", dateFrom, timeFrom, dateTo);
                                    localNLog.Warn("Failed to find last created datetime, using following : \ndateFrom {0}\ntimeFrom {0}\ndateTo {0}", dateFrom, timeFrom, dateTo);
                                    LoggerSQL.Warn(string.Format("Failed to find last created datetime, using following : \ndateFrom {0}\ntimeFrom {0}\ndateTo {0}", dateFrom, timeFrom, dateTo));
                                }
                            }
                            #endregion

                            #region Retrieve Assignements and Claim Detail Updates

                            //Retrieve Assignements from API
                            if (smallClaimCalls.ServiceCall("Get Assignments", null, "dateFrom", dateFrom, "timeFrom", timeFrom))
                            {
                                Console.WriteLine(smallClaimCalls.APIResponse);
                                localNLog.Info(smallClaimCalls.APIResponse);
                                LoggerSQL.Info(smallClaimCalls.APIResponse.ToString(Formatting.None));

                                if (smallClaimCalls.APIResponse.ToString(Formatting.None).Contains("assignments") && smallClaimCalls.APIResponse["assignments"].ToString(Formatting.None).Contains("claimID"))
                                {
                                    List<List<string>> assignmentslist = new List<List<string>>();
                                    dynamic assingn = smallClaimCalls.APIResponse["assignments"];
                                    foreach (dynamic assignment in smallClaimCalls.APIResponse["assignments"])
                                    {
                                        string[] assignval = new string[7];
                                        assignval[0] = assignment.claimID;
                                        assignval[1] = assignment.assignmentID;
                                        assignval[2] = assignment.instructions;
                                        assignval[3] = dynDateToString(assignment, "createdDateTime");
                                        assignval[4] = dynDateToString(assignment, "deadlineDateTime");
                                        assignval[5] = dynDateToString(assignment, "goalDateTime");
                                        assignval[6] = assignment.ToString(Formatting.None);

                                        assignmentslist.Add(assignval.ToList());
                                    }

                                    foreach (List<string> assignment in assignmentslist)
                                    {
                                        // get small claims info from claim ID
                                        string claimid = assignment[0];
                                        Entity smallclaiminfo;

                                        if (claimid.Contains("MED"))
                                        {
                                            if (smallClaimCalls.ServiceCall("Get Claim Details", null, "claimID", claimid))
                                            {
                                                Console.WriteLine(smallClaimCalls.APIResponse);
                                                localNLog.Info(smallClaimCalls.APIResponse);
                                                
                                                claimid = smallClaimCalls.APIResponse["medicalClaimDetails"]["parentClaimID"];
                                            }

                                            smallclaiminfo = helpers.generalQuery("nal_smallclaiminformation", new ColumnSet("nal_smallclaiminformationid", "nal_defendantid", "nal_matterid", "nal_applicationid", "nal_smallclaimsstage", "nal_compensator", "nal_medid"), new List<ConditionExpression>() {
                                            new ConditionExpression("nal_applicationid", ConditionOperator.Equal, claimid) }
                                            ).Entities.FirstOrDefault();

                                        }
                                        else if (claimid.Contains("CL"))
                                        {
                                            Entity smallclaimsliability = helpers.generalQuery("nal_smallclaimsliability", new ColumnSet("nal_smallclaiminformationid", "nal_claimid"), new List<ConditionExpression>() {
                                            new ConditionExpression("nal_challengeliabilityclaimid", ConditionOperator.Equal, claimid) }
                                            ).Entities.FirstOrDefault();

                                            smallclaiminfo = helpers.generalQuery("nal_smallclaiminformation", new ColumnSet("nal_smallclaiminformationid", "nal_defendantid", "nal_matterid", "nal_applicationid", "nal_smallclaimsstage", "nal_compensator", "nal_medid"), new List<ConditionExpression>() {
                                            new ConditionExpression("nal_applicationid", ConditionOperator.Equal, smallclaimsliability.GetAttributeValue<string>("nal_claimid")) }
                                            ).Entities.FirstOrDefault();
                                        }
                                        else
                                        {
                                            smallclaiminfo = helpers.generalQuery("nal_smallclaiminformation", new ColumnSet("nal_smallclaiminformationid", "nal_defendantid", "nal_matterid", "nal_applicationid", "nal_smallclaimsstage", "nal_compensator", "nal_medid"), new List<ConditionExpression>() {
                                            new ConditionExpression("nal_applicationid", ConditionOperator.Equal, claimid) }
                                            ).Entities.FirstOrDefault();
                                        }

                                        if (smallclaiminfo != null)
                                        {
                                            // get small claims history record that matches assignement ID
                                            Entity existingRecords = helpers.generalQuery("nal_smallclaimshistory", new ColumnSet("nal_apirecordid"), new List<ConditionExpression>() {
                                                new ConditionExpression("nal_apirecordid", ConditionOperator.Equal, assignment[1]) /*if assignmentID matches*/,
                                                new ConditionExpression("nal_apicreateddatetime", ConditionOperator.Equal, assignment[3] /*if createdDateTime matches*/)
                                                }
                                            ).Entities.FirstOrDefault();

                                            //if assignement is new
                                            if (existingRecords == null)
                                            {
                                                createSmallClaimsHistory(
                                                    smallclaiminfo,
                                                    assignment[0]/*claimID*/,
                                                    "Assignment found : " + assignment[1]/*assignmentID*/,
                                                    string.Format(
                                                        "Assignment found for Claim ID : {0}\nAssignment ID : {1}\nAssignment Instructions : {2}\nAssignment Date Created : {3}\nAssignment Date Deadline : {4}\nAssignment Goal Datetime : {5}",
                                                        assignment[0],
                                                        assignment[1],
                                                        assignment[2],
                                                        assignment[3],
                                                        assignment[4],
                                                        assignment[5]),
                                                    "Small Claims Console Logs",
                                                    assignment[0] /*assignment.ToString - API Response*/,
                                                    assignment[1] /*assignmentID*/,
                                                    assignment[3] /*createdDateTime*/
                                                );

                                                //Retrieve Claim details
                                                if (smallClaimCalls.ServiceCall("Get Claim Details", null, "claimID", claimid))
                                                {
                                                    Console.WriteLine(smallClaimCalls.APIResponse);
                                                    localNLog.Info(smallClaimCalls.APIResponse);

                                                    if (!claimid.Contains("CL"))
                                                    {
                                                        updateDefendantRecord(smallclaiminfo, claimid, smallClaimCalls.APIResponse["claimDetails"]);
                                                        checkLiabilityInformation(smallclaiminfo, claimid, assignment[1], smallClaimCalls.APIResponse["claimDetails"]);

                                                        string claimstatus = smallClaimCalls.APIResponse["claimStatus"];
                                                        updateSmallClaimInformation(smallclaiminfo, claimid, claimstatus, smallClaimCalls.APIResponse, smallClaimCalls.APIResponse.ToString(Formatting.None));

                                                        checkForNewMessages(smallClaimCalls, smallclaiminfo, claimid);
                                                        checkForNewDocuments(smallClaimCalls, smallclaiminfo, claimid);
                                                    }
                                                    else
                                                    {
                                                        updateDefendantRecord(smallclaiminfo, claimid, smallClaimCalls.APIResponse["parentClaimDetails"]["claimDetails"]);
                                                        checkLiabilityInformation(smallclaiminfo, claimid, assignment[1], smallClaimCalls.APIResponse["parentClaimDetails"]["claimDetails"], smallClaimCalls.APIResponse["challengeLiabilityDetails"]);

                                                        string claimstatus = smallClaimCalls.APIResponse["parentClaimDetails"]["claimStatus"];
                                                        updateSmallClaimInformation(smallclaiminfo, claimid, claimstatus, smallClaimCalls.APIResponse, smallClaimCalls.APIResponse.ToString(Formatting.None));

                                                        checkForNewMessages(smallClaimCalls, smallclaiminfo, claimid);
                                                        checkForNewDocuments(smallClaimCalls, smallclaiminfo, claimid);
                                                    }
                                                }
                                                else
                                                {
                                                    Console.WriteLine(smallClaimCalls.APIResponse);
                                                    localNLog.Warn("Error : SmallClaimAPICalls failed ServiceCall() Method! = {0} : {1}\n Service Reply : {2}", "Get Claim Details", claimid, smallClaimCalls.APIResponse);
                                                    LoggerSQL.Warn(string.Format("Error : SmallClaimAPICalls failed ServiceCall() Method! = {0} : {1}\n Service Reply : {2}", "Get Claim Details", claimid, smallClaimCalls.APIResponse));
                                                }
                                            }
                                        }
                                        else
                                        {
                                            Console.WriteLine("Error : Small Claims not found for Claim ID = {0}", assignment[0]);
                                            localNLog.Fatal("Error : Small Claims not found for Claim ID = {0}", assignment[0]);
                                            LoggerSQL.Fatal(string.Format("Error : Small Claims not found for Claim ID = {0}", assignment[0]));
                                        }
                                    }
                                }
                                else
                                {
                                    Console.WriteLine("No assignments found on query Get Assignments : from date {0} time {1}", dateFrom, timeFrom);
                                    localNLog.Info("No assignments found on query Get Assignments : from date {0} time {1}", dateFrom, timeFrom);
                                    LoggerSQL.Info(string.Format("No assignments found on query Get Assignments : from date {0} time {1}", dateFrom, timeFrom));
                                }

                            }
                            else
                            {
                                Console.WriteLine(smallClaimCalls.APIResponse);
                                localNLog.Fatal("Error : SmallClaimAPICalls failed ServiceCall() Method! = {0} : {1}\n Service Reply : {2}", "Get Assignments", dateFrom, smallClaimCalls.APIResponse);
                                LoggerSQL.Fatal(string.Format("Error : SmallClaimAPICalls failed ServiceCall() Method! = {0} : {1}\n Service Reply : {2}", "Get Assignments", dateFrom, smallClaimCalls.APIResponse));
                            }

                            #endregion

                            #region Retrieve Notifications with downstream records (Messages, Documents, (Losses not currently being retrived))

                            if (smallClaimCalls.ServiceCall("Get Notifications", null, "dateFrom", dateFrom, "dateTo", dateTo))
                            {

                                Console.WriteLine(smallClaimCalls.APIResponse);
                                localNLog.Info(smallClaimCalls.APIResponse);
                                LoggerSQL.Info(smallClaimCalls.APIResponse.ToString(Formatting.None));

                                if (smallClaimCalls.APIResponse.ToString(Formatting.None).Contains("notifications"))
                                {

                                    List<List<string>> notificationslist = new List<List<string>>();
                                    foreach (dynamic notification in smallClaimCalls.APIResponse["notifications"])
                                    {
                                        string[] notificationval = new string[4];
                                        notificationval[0] = notification.claimID;
                                        notificationval[1] = notification.NotificationType;
                                        notificationval[2] = dynDateToString(notification, "createdDateTime");
                                        notificationval[3] = notification.ToString(Formatting.None);

                                        notificationslist.Add(notificationval.ToList());
                                    }

                                    notificationslist = notificationslist.OrderBy(n => n[0]).ToList();

                                    List<string> processedClaimNotifications = new List<string>();
                                    foreach (List<string> notification in notificationslist)
                                    {

                                        if (!processedClaimNotifications.Contains(notification[0] + " " + notification[1]))
                                        {
                                            string claimid = notification[0];
                                            string notificationType = notification[1];
                                            string createdDateTime = notification[2];
                                            string notificationAPIResponse = notification[3];
                                            processedClaimNotifications.Add(claimid + " " + notificationType);

                                            Entity smallclaiminfo = helpers.generalQuery("nal_smallclaiminformation", new ColumnSet(true), new List<ConditionExpression>() {
                                                new ConditionExpression("nal_applicationid", ConditionOperator.Equal, claimid) }
                                            ).Entities.FirstOrDefault();

                                            if (smallclaiminfo != null)
                                            {

                                                if (notificationType == "A new message has been added to the claim")
                                                {
                                                    ////Retrieve Message List
                                                    checkForNewMessages(smallClaimCalls, smallclaiminfo, claimid);
                                                }

                                                if (notificationType == "A new document has been added to the claim")
                                                {
                                                    ////Retrieve Attachement List
                                                    checkForNewDocuments(smallClaimCalls, smallclaiminfo, claimid);
                                                }

                                                if (notificationType == "New loss(es) have been added to the claim")
                                                {
                                                    Console.WriteLine("Warn : New loss(es) : No data processed");
                                                }
                                            }
                                            else
                                            {
                                                Console.WriteLine("Fatal : Small Claims not found for Claim ID = {0} :: Notification = {1}", claimid, notificationType);
                                                localNLog.Fatal("Fatal : Small Claims not found for Claim ID = {0} :: Notification = {1}", claimid, notificationType);
                                                LoggerSQL.Fatal(string.Format("Fatal : Small Claims not found for Claim ID = {0} :: Notification = {1}", claimid, notificationType));
                                            }
                                        }
                                    }
                                }
                                else
                                {
                                    Console.WriteLine("No notification found on query Get Notifications : from {0} to {1}", dateFrom, dateTo);
                                    localNLog.Info("No notification found on query Get Notifications : from {0} to {1}", dateFrom, dateTo);
                                    LoggerSQL.Info(string.Format("No notification found on query Get Notifications : from {0} to {1}", dateFrom, dateTo));
                                }
                            }
                            else
                            {
                                Console.WriteLine(smallClaimCalls.APIResponse);
                                localNLog.Fatal("Error : SmallClaimAPICalls failed ServiceCall() Method! = {0} : {1}\n Service Reply : {2}", "Get Notifications", dateFrom, smallClaimCalls.APIResponse);
                                LoggerSQL.Fatal(string.Format("Error : SmallClaimAPICalls failed ServiceCall() Method! = {0} : {1}\n Service Reply : {2}", "Get Notifications", dateFrom, smallClaimCalls.APIResponse));
                            }

                            #endregion

                            #region Check for Compensator assignements (Notifications Not Provided by OIC 02/06/2021)

                            EntityCollection smallclaiminfoLatestSubmissions = helpers.generalQuery("nal_smallclaimshistory",
                                new ColumnSet("nal_smallclaiminformation"),
                                new List<ConditionExpression>() {
                                    new ConditionExpression("nal_name", ConditionOperator.Like, "Successful - Call Create Claim%"),
                                    new ConditionExpression("createdon", ConditionOperator.GreaterEqual, DateTime.Now.AddDays(-7)),
                                    new ConditionExpression("createdon", ConditionOperator.LessEqual, DateTime.Now.AddDays(-1)),
                                }
                            );

                            EntityCollection smallclaiminfoPendingAssignement = helpers.generalQuery("nal_smallclaiminformation",
                                new ColumnSet("nal_smallclaiminformationid", "nal_defendantid", "nal_matterid", "nal_applicationid", "nal_smallclaimsstage", "nal_compensator", "nal_medid"),
                                new List<ConditionExpression>() {
                                    new ConditionExpression("nal_smallclaimsstage", ConditionOperator.Equal, "Pending-CompensatorAssignment"),
                                    new ConditionExpression("nal_smallclaiminformationid", ConditionOperator.In, smallclaiminfoLatestSubmissions.Entities.Select(s=>s.GetAttributeValue<EntityReference>("nal_smallclaiminformation").Id.ToString()).ToList())
                                }
                            );


                            if (smallclaiminfoPendingAssignement != null)
                            {
                                foreach (Entity smallclaiminfo in smallclaiminfoPendingAssignement.Entities)
                                {
                                    //Retrieve Compensator assignements without notifications
                                    string claimid = smallclaiminfo.GetAttributeValue<string>("nal_applicationid");
                                    if (smallclaiminfo != null && claimid != null)
                                    {
                                        //Retrieve Claim details
                                        if (smallClaimCalls.ServiceCall("Get Claim Details", null, "claimID", claimid))
                                        {
                                            Console.WriteLine(smallClaimCalls.APIResponse);
                                            localNLog.Info(smallClaimCalls.APIResponse);

                                            updateDefendantRecord(smallclaiminfo, claimid, smallClaimCalls.APIResponse["claimDetails"]);

                                            string claimstatus = smallClaimCalls.APIResponse["claimStatus"];
                                            updateSmallClaimInformation(smallclaiminfo, claimid, claimstatus, smallClaimCalls.APIResponse, smallClaimCalls.APIResponse.ToString(Formatting.None));

                                            try
                                            {
                                                checkForNewMessages(smallClaimCalls, smallclaiminfo, claimid);
                                                checkForNewDocuments(smallClaimCalls, smallclaiminfo, claimid);
                                            }
                                            catch 
                                            {
                                                //Issue with certain claims at this stage through API for retrieving messages. Purpose of this is purely to update compensator details at an earlier point so can just be skipped in these circumstances
                                            }
                                            
                                        }
                                        else
                                        {
                                            Console.WriteLine(smallClaimCalls.APIResponse);
                                            throw new Exception("SmallClaimAPICalls failed ServiceCall() Method!");
                                        }
                                    }
                                    else
                                    {
                                        Console.WriteLine("Error : Small Claims not found for Claim ID = {0}", claimid);
                                        localNLog.Warn("Error : Small Claims not found for Claim ID = {0}", claimid);
                                    }
                                }
                            }
                            #endregion
                        }
                        else
                        {
                            localNLog.Info("Claims Override in place for Claim ID : {0}", overrideClaimID);

                            // get small claims info from claim ID
                            string claimid = overrideClaimID;
                            Entity smallclaiminfo;

                            if (claimid.Contains("CL"))
                            {
                                Entity smallclaimsliability = helpers.generalQuery("nal_smallclaimsliability", new ColumnSet("nal_smallclaiminformationid", "nal_claimid"), new List<ConditionExpression>() {
                                            new ConditionExpression("nal_challengeliabilityclaimid", ConditionOperator.Equal, claimid) }
                                ).Entities.FirstOrDefault();

                                smallclaiminfo = helpers.generalQuery("nal_smallclaiminformation", new ColumnSet("nal_smallclaiminformationid", "nal_defendantid", "nal_matterid", "nal_applicationid", "nal_smallclaimsstage", "nal_compensator", "nal_medid"), new List<ConditionExpression>() {
                                            new ConditionExpression("nal_applicationid", ConditionOperator.Equal, smallclaimsliability.GetAttributeValue<string>("nal_claimid")) }
                                ).Entities.FirstOrDefault();
                            }
                            else
                            {
                                smallclaiminfo = helpers.generalQuery("nal_smallclaiminformation", new ColumnSet("nal_smallclaiminformationid", "nal_defendantid", "nal_matterid", "nal_applicationid", "nal_smallclaimsstage", "nal_compensator", "nal_medid"), new List<ConditionExpression>() {
                                            new ConditionExpression("nal_applicationid", ConditionOperator.Equal, claimid) }
                                ).Entities.FirstOrDefault();
                            }

                            #region Get Claim Details

                            if (smallclaiminfo != null)
                            {
                                    //Retrieve Claim details
                                    if (smallClaimCalls.ServiceCall("Get Claim Details", null, "claimID", claimid))
                                    {
                                        Console.WriteLine(smallClaimCalls.APIResponse);
                                        localNLog.Info(smallClaimCalls.APIResponse);

                                        if (!claimid.Contains("CL"))
                                        {
                                            updateDefendantRecord(smallclaiminfo, claimid, smallClaimCalls.APIResponse["claimDetails"]);
                                            checkLiabilityInformation(smallclaiminfo, claimid, "noassignment", smallClaimCalls.APIResponse["claimDetails"]);

                                            string claimstatus = smallClaimCalls.APIResponse["claimStatus"];
                                            updateSmallClaimInformation(smallclaiminfo, claimid, claimstatus, smallClaimCalls.APIResponse, smallClaimCalls.APIResponse.ToString(Formatting.None));

                                            checkForNewMessages(smallClaimCalls, smallclaiminfo, claimid);
                                            checkForNewDocuments(smallClaimCalls, smallclaiminfo, claimid);

                                        }
                                        else
                                        {
                                            updateDefendantRecord(smallclaiminfo, claimid, smallClaimCalls.APIResponse["parentClaimDetails"]["claimDetails"]);
                                            checkLiabilityInformation(smallclaiminfo, claimid, "noassignment", smallClaimCalls.APIResponse["parentClaimDetails"]["claimDetails"]);

                                            string claimstatus = smallClaimCalls.APIResponse["parentClaimDetails"]["claimStatus"];
                                            updateSmallClaimInformation(smallclaiminfo, claimid, claimstatus, smallClaimCalls.APIResponse, smallClaimCalls.APIResponse.ToString(Formatting.None));

                                            checkForNewMessages(smallClaimCalls, smallclaiminfo, claimid);
                                            checkForNewDocuments(smallClaimCalls, smallclaiminfo, claimid);
                                        }
                                    }
                                    else
                                    {
                                        Console.WriteLine(smallClaimCalls.APIResponse);
                                        throw new Exception("SmallClaimAPICalls failed ServiceCall() Method!");
                                    }
                            }
                            else
                            {
                                Console.WriteLine("Error : Small Claims not found for Claim ID = {0}", claimid);
                                localNLog.Warn("Error : Small Claims not found for Claim ID = {0}", claimid);
                            }

                            #endregion

                            #region Get Message List & Docs
                            //Retrieve Message List
                            checkForNewMessages(smallClaimCalls, smallclaiminfo, claimid);

                            //Retrieve Attachement List
                            checkForNewDocuments(smallClaimCalls, smallclaiminfo, claimid);
                            #endregion
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Main Body : Exception Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
                        localNLog.Fatal("Main Body : Exception Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
                        LoggerSQL.Fatal("Main Body : Exception Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
                    }
                }
                else
                {
                    Console.WriteLine("Unable to connect to CRM Dynamics Org : " + connectOrg);
                    localNLog.Fatal("Unable to connect to CRM Dynamics Org : " + connectOrg);
                    LoggerSQL.Fatal("Unable to connect to CRM Dynamics Org : " + connectOrg);
                }

            }

            if (IsTesting)
            {
                Console.WriteLine("Program has ended press any key to terminate");
                Console.ReadKey();
            }
        }

        private static T getDynamicField<T>(
            Entity entity, 
            string attributeName, 
            dynamic dynamicObject, 
            string fieldName)
        {
            string fieldValue = dynToString(dynamicObject[fieldName]);

            if(fieldValue == null)
                return default(T);

            if (typeof(T) == typeof(string))
            {
                return (T)Convert.ChangeType(fieldValue, typeof(T));
                //return (T)fieldValue;
            }
            else if (typeof(T) == typeof(DateTime))
            {
                return (T)(object)DateTime.Parse(fieldValue);
            }
            else if (typeof(T) == typeof(Int32))
            {
                return (T)(object)Int32.Parse(fieldValue);
            }
            else if(typeof(T).ToString() == "Microsoft.Xrm.Sdk.OptionSetValue")
            {
                return (T)(object) new OptionSetValue(helpers.GetOptionSetValueFromLabel(entity.LogicalName, attributeName, fieldValue));
            }

            return default(T);
        }

        private static string dynToString(
            dynamic dynamic)
        {
            string s = dynamic;

            if(s == "True")
                return "Yes";

            if (s == "False")
                return "No";

            return s;
        }

        private static string dynDateToString(dynamic dynamicObject, string fieldName) => dynamicObject[fieldName] == null ? null : dynamicObject[fieldName].ToString(Formatting.None).Trim('"');

        private static string getMedicalId(
            dynamic dynamicObject)
        {
            try
            {
                string fieldValue = null;
                if (dynamicObject.ToString(Formatting.None).Contains("medicalClaimDetails"))
                {
                    fieldValue = dynToString(dynamicObject["medicalClaimDetails"]["medicalClaimID"]);
                }
                else if (dynamicObject.ToString(Formatting.None).Contains("medicalCaseKey"))
                {
                    fieldValue = dynToString(dynamicObject["medicalCaseKey"]);
                }
                else if (dynamicObject.ToString(Formatting.None).Contains("medicalID"))
                {
                    fieldValue = dynToString(dynamicObject["medical"][0]["medicalID"]);
                }

                return fieldValue;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed return Medical Id:\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
                localNLog.Warn("Failed return Medical Id:\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
                LoggerSQL.Warn("Failed return Medical Id:\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
            }

            return null;
        }

        private static string StripHTML(
            string input)
        {
            return Regex.Replace(input, "<.*?>", String.Empty);
        }

        private static void checkForNewMessages(
            SmallClaimAPICalls smallClaimCalls, 
            Entity smallclaiminfo, 
            string claimid)
        {
            //Retrieve Message List
            if (smallClaimCalls.ServiceCall("Get Message List", null, "claimID", claimid))
            {

                Console.WriteLine(smallClaimCalls.APIResponse);
                localNLog.Info(smallClaimCalls.APIResponse);

                List<List<string>> messagelist = new List<List<string>>();
                foreach (dynamic message in smallClaimCalls.APIResponse["results"])
                {
                    string[] messageval = new string[3];
                    messageval[0] = message.id;
                    messageval[1] = message.category;
                    messageval[2] = dynDateToString(message, "createDateTime");

                    messagelist.Add(messageval.ToList());
                }

                EntityCollection existingRecords = helpers.generalQuery("nal_smallclaimshistory", new ColumnSet("nal_apirecordid"), new List<ConditionExpression>() {
                                                            new ConditionExpression("nal_apirecordid", ConditionOperator.In, messagelist.Select(m=>m[0]).ToArray()) }
                );

                EntityCollection existingMessages = helpers.generalQuery("nal_smallclaimsmessage", new ColumnSet(true), new List<ConditionExpression>() {
                                                        new ConditionExpression("nal_claimid", ConditionOperator.Equal, smallclaiminfo.GetAttributeValue<string>("nal_applicationid")) }
                );

                int newMessageCount = (messagelist.Count() - existingMessages.Entities.Count());
                if (newMessageCount > 0)
                {
                    createSmallClaimsHistory(
                        smallclaiminfo,
                        claimid,
                        newMessageCount > 1 ? newMessageCount + " Messages found" : "Message found",
                        string.Format("Message found for Claim ID : {0}",
                            claimid
                        ),
                        "Small Claims Console Logs",
                        smallClaimCalls.APIResponse["results"].ToString(Formatting.None),
                        null,
                        null
                    );

                    foreach (List<string> message in messagelist)
                    {
                        if (existingMessages.Entities.Where(e => e.GetAttributeValue<string>("nal_messageid") == message[0]).Count() == 0)
                        {
                            if (smallClaimCalls.ServiceCall("Get Message", null, "messageID", message[0]))
                            {
                                Console.WriteLine(smallClaimCalls.APIResponse);
                                localNLog.Info(smallClaimCalls.APIResponse);

                                var messageInfo = smallClaimCalls.APIResponse["messageInfo"];
                                string author = smallClaimCalls.APIResponse["author"];
                                string note = smallClaimCalls.APIResponse["note"];
                                note = StripHTML(note);

                                string category = messageInfo.category;
                                string createDateTime = dynDateToString(messageInfo, "createdDateTime");
                                string messageId = messageInfo.id;

                                if (author == "Official Injury Claim" || author == smallclaiminfo.GetAttributeValue<string>("nal_compensator") || existingMessages.Entities.Where(w => w.GetAttributeValue<string>("description") == note && w.GetAttributeValue<string>("nal_categorytext") == category).Count() == 0)
                                {

                                    createMessageActivity(
                                        smallclaiminfo,
                                        claimid,
                                        messageId,
                                        createDateTime,
                                        category,
                                        author,
                                        note,
                                        smallClaimCalls
                                    );

                                    if (category == "Claim Removed")
                                    {
                                        //Retrieve Claim details
                                        if (smallClaimCalls.ServiceCall("Get Claim Details", null, "claimID", claimid))
                                        {
                                            Console.WriteLine(smallClaimCalls.APIResponse);

                                            string claimstatus = smallClaimCalls.APIResponse["claimStatus"];
                                            dynamic claimDetails = smallClaimCalls.APIResponse["claimDetails"];

                                            updateSmallClaimInformationClaimRemoved(smallclaiminfo, claimid, claimstatus, createDateTime, claimDetails, smallClaimCalls.APIResponse.ToString(Formatting.None));

                                        }
                                    }

                                    if (category == "Liability decision")
                                    {
                                        //Set "Liability Accepted by OIC System Timeout?" field to Yes
                                        if (note.Contains("No response has been provided by the compensator within the relevant response period.\n\nThe claim is then treated as one where liability has been admitted in full by the compensator.") ||
                                            note.Contains("Because the compensator has not made the liability decision within the expected time, the service has accepted liability on their behalf."))
                                        {
                                            updateSmallClaimInformationSystemAcceptedLiability(smallclaiminfo, claimid);
                                        }
                                    }

                                    if(category == "CompResponseCategory")
                                    {
                                        if (note.Contains("The compensator has reviewed the liability challenge.\n\nThe decision is: They have now accepted liability in full.\n&nbsp;"))
                                        {
                                            #region Get Claim Details

                                            //Retrieve Claim details
                                            if (smallClaimCalls.ServiceCall("Get Claim Details", null, "claimID", claimid))
                                            {
                                                Console.WriteLine(smallClaimCalls.APIResponse);
                                                localNLog.Info(smallClaimCalls.APIResponse);

                                                    updateDefendantRecord(smallclaiminfo, claimid, smallClaimCalls.APIResponse["claimDetails"]);
                                                    checkLiabilityInformation(smallclaiminfo, claimid, "noassignment", smallClaimCalls.APIResponse["claimDetails"]);

                                                    string claimstatus = smallClaimCalls.APIResponse["claimStatus"];
                                                    updateSmallClaimInformation(smallclaiminfo, claimid, claimstatus, smallClaimCalls.APIResponse, smallClaimCalls.APIResponse.ToString(Formatting.None));

                                                    checkForNewDocuments(smallClaimCalls, smallclaiminfo, claimid);
                                            }
                                            else
                                            {
                                                Console.WriteLine(smallClaimCalls.APIResponse);
                                                localNLog.Warn("Error : SmallClaimAPICalls failed ServiceCall() Method! = {0} : {1}\n Service Reply : {2}", "Get Claim Details (Message - Liability admitted in full)", claimid, smallClaimCalls.APIResponse);
                                                LoggerSQL.Warn(string.Format("Error : SmallClaimAPICalls failed ServiceCall() Method! = {0} : {1}\n Service Reply : {2}", "Get Claim Details (Message - Liability admitted in full)", claimid, smallClaimCalls.APIResponse));
                                            }
                                            #endregion
                                        }

                                    }

                                }
                            }
                            else
                            {
                                Console.WriteLine(smallClaimCalls.APIResponse);
                                localNLog.Warn("Error : SmallClaimAPICalls failed ServiceCall() Method! = {0} : {1}\n Service Reply : {2}", "Get Message", message[0], smallClaimCalls.APIResponse);
                                LoggerSQL.Warn(string.Format("Error : SmallClaimAPICalls failed ServiceCall() Method! = {0} : {1}\n Service Reply : {2}", "Get Message", message[0], smallClaimCalls.APIResponse));
                            }
                        }
                    }
                }
            }
            else
            {
                Console.WriteLine(smallClaimCalls.APIResponse);
                localNLog.Warn("Error : SmallClaimAPICalls failed ServiceCall() Method! = {0} : {1}\n Service Reply : {2}", "Get Message List", claimid, smallClaimCalls.APIResponse);
                LoggerSQL.Warn(string.Format("Error : SmallClaimAPICalls failed ServiceCall() Method! = {0} : {1}\n Service Reply : {2}", "Get Message List", claimid, smallClaimCalls.APIResponse));
            }

        }

        private static void checkForNewDocuments(
            SmallClaimAPICalls smallClaimCalls, 
            Entity smallclaiminfo, 
            string claimid)
        {

            //Retrieve Attachement List
            if (smallClaimCalls.ServiceCall("Get Attachment List", null, "claimID", claimid))
            {
                Console.WriteLine(smallClaimCalls.APIResponse);
                localNLog.Info(smallClaimCalls.APIResponse);

                List<List<string>> doclist = new List<List<string>>();
                foreach (dynamic doc in smallClaimCalls.APIResponse["results"])
                {
                    string[] docval = new string[6];
                    docval[0] = doc.id;
                    docval[1] = doc.category;
                    docval[2] = doc.fileName;
                    docval[3] = doc.memo;
                    docval[4] = dynDateToString(doc, "createDateTime");
                    docval[5] = doc.ToString(Formatting.None);

                    doclist.Add(docval.ToList());
                }

                EntityCollection existingRecords = helpers.generalQuery("nal_smallclaimshistory", new ColumnSet("nal_apirecordid"), new List<ConditionExpression>() {
                                                    new ConditionExpression("nal_apirecordid", ConditionOperator.In, doclist.Select(m=>m[0]).ToArray()) }
                );

                EntityCollection existingDocuments = helpers.generalQuery("letter", new ColumnSet(true), new List<ConditionExpression>() {
                                                    new ConditionExpression("nal_smallclaimsdocument", ConditionOperator.Equal, true),
                                                    new ConditionExpression("regardingobjectid", ConditionOperator.Equal, smallclaiminfo.GetAttributeValue<EntityReference>("nal_matterid").Id.ToString())
                                    }
                );

                int newDocsCount = (doclist.Count() - existingDocuments.Entities.Where(d => d.GetAttributeValue<string>("subject").Contains(claimid)).Count());
                if (newDocsCount > 0)
                {

                    createSmallClaimsHistory(
                        smallclaiminfo,
                        claimid,
                        newDocsCount > 1 ? newDocsCount + " Documents found" : "Document found",
                        string.Format("Document found for Claim ID : {0}",
                            claimid
                        ),
                        "Small Claims Console Logs",
                        smallClaimCalls.APIResponse["results"].ToString(Formatting.None),
                        null,
                        null
                    );

                    foreach (List<string> doc in doclist)
                    {
                        if (existingRecords.Entities.Where(e => e.GetAttributeValue<string>("nal_apirecordid") == doc[0] /*Doc ID*/).Count() == 0)
                        {
                            if (smallClaimCalls.ServiceCall("Get Attachment by ID", null, "attachmentID", doc[0]/*Doc ID*/))
                            {
                                Console.WriteLine(smallClaimCalls.APIResponse["fileInfo"]);
                                localNLog.Info(smallClaimCalls.APIResponse["fileInfo"]);

                                dynamic document = smallClaimCalls.APIResponse["fileInfo"];
                                string filestream = smallClaimCalls.APIResponse["fileStream"];

                                string filename = document.fileName;
                                string created = dynDateToString(document, "createdDateTime");
                                string memo = document.memo;
                                string doccategory = document.category;
                                string docid = document.id;
                                string docbytes = filestream;
                                string mimetype = filename.Substring(filename.LastIndexOf(".") + 1);
                                string subject = "OIC Portal - " + filename.Substring(0, filename.LastIndexOf("."));

                                if (existingDocuments.Entities.Where(w => w.GetAttributeValue<string>("subject").Contains(filename) && w.GetAttributeValue<string>("subject").Contains("Upload")).Count() == 0)
                                {

                                    try
                                    {
                                        SharepointDocuments docProcessor = new SharepointDocuments(service, helpers);
                                        docProcessor.CreateDocumentProcessor(smallclaiminfo.GetAttributeValue<EntityReference>("nal_matterid"), subject, filename, docbytes, mimetype);

                                        createSmallClaimsHistory(
                                            smallclaiminfo,
                                            claimid,
                                            "Document retrieved successfully",
                                            string.Format("Document File name : {0}\nDocument Subject : {1}\nCreated Date Time : {2}\nMemo : {3}\nDocument ID : {4}",
                                                filename,
                                                subject,
                                                created,
                                                memo,
                                                docid
                                            ),
                                            "Small Claims Console Logs",
                                            smallClaimCalls.APIResponse["fileInfo"].ToString(Formatting.None),
                                            docid,
                                            created
                                        );

                                    }
                                    catch (Exception ex)
                                    {
                                        createSmallClaimsHistory(
                                            smallclaiminfo,
                                            claimid,
                                            "Failed to return document",
                                            string.Format("Document File name : {0}\nDocument Subject : {1}\nCreated Date Time : {2}\nMemo : {3}\nDocumant ID : {4}",
                                                filename,
                                                subject,
                                                created,
                                                memo,
                                                docid
                                            ),
                                            "Failed to return document:\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace,
                                            smallClaimCalls.APIResponse["fileInfo"].ToString(Formatting.None),
                                            null,
                                            null
                                        );

                                    }

                                }
                            }
                            else
                            {
                                Console.WriteLine(smallClaimCalls.APIResponse);
                                localNLog.Warn("Error : SmallClaimAPICalls failed ServiceCall() Method! = {0} : {1}\n Service Reply : {2}", "Get Attachment by ID", doc[0], smallClaimCalls.APIResponse);
                                LoggerSQL.Warn(string.Format("Error : SmallClaimAPICalls failed ServiceCall() Method! = {0} : {1}\n Service Reply : {2}", "Get Attachment by ID", doc[0], smallClaimCalls.APIResponse));
                            }
                        }
                    }
                }

            }
            else
            {
                Console.WriteLine(smallClaimCalls.APIResponse);
                localNLog.Warn("Error : SmallClaimAPICalls failed ServiceCall() Method! = {0} : {1}\n Service Reply : {2}", "Get Attachment List", claimid, smallClaimCalls.APIResponse);
                LoggerSQL.Warn(string.Format("Error : SmallClaimAPICalls failed ServiceCall() Method! = {0} : {1}\n Service Reply : {2}", "Get Attachment List", claimid, smallClaimCalls.APIResponse));
            }
        }

        private static void updateDefendantRecord(
            Entity smallclaiminfo, 
            string claimid, 
            dynamic claimDetails)
        {
            try 
            {

                EntityReference defendantreference = smallclaiminfo.GetAttributeValue<EntityReference>("nal_defendantid");
                if (defendantreference != null) 
                {

                    string claimReferenceNumber = claimDetails.claimReferenceNumber;
                    string claimHandlerName = claimDetails.claimHandlerName;
                    string claimHandlerPhone = claimDetails.claimHandlerPhone;
                    string claimHandlerEmail = claimDetails.claimHandlerEmail;

                    string claimInsurerPolicyNumber = claimDetails.insurerPolicyNumber;
                    string claimInsurerId = claimDetails.insurerId;
                    string claimIsCompensatorMIB = claimDetails.isCompensatorMIB;


                    Entity defendant = new Entity("pi_defendant");
                    defendant.Attributes["pi_defendantid"] = defendantreference.Id;

                    if (claimReferenceNumber != null)
                        defendant.Attributes["pi_referencenumber"] = claimReferenceNumber;

                    if (claimHandlerName != null)
                        defendant.Attributes["pi_contactname"] = claimHandlerName;

                    if (claimHandlerPhone != null)
                        defendant.Attributes["pi_telephonenumber"] = claimHandlerPhone;

                    if (claimHandlerEmail != null)
                        defendant.Attributes["pi_emailaddress"] = claimHandlerEmail;

                    if (claimInsurerId != null)
                        defendant.Attributes["pi_defendantinsurerreference"] = claimInsurerId;

                    if (claimInsurerPolicyNumber != null)
                        defendant.Attributes["pi_defendantinsurerpolicynumber"] = claimInsurerPolicyNumber;
                    
                    if(defendant.Attributes.Count() > 1) 
                    { 
                        service.Update(defendant);

                        Console.WriteLine("Defendant Updated : " + defendantreference.Id);
                        localNLog.Info("Defendant Updated : " + defendantreference.Id);
                    }
                }
                else
                {
                    Console.WriteLine("Defendant not found in Small Claims : " + smallclaiminfo.Id);
                    localNLog.Warn("Defendant not found in Small Claims : " + smallclaiminfo.Id);
                    LoggerSQL.Warn("Defendant not found in Small Claims : " + smallclaiminfo.Id);
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine("Failed to update Defendant:\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
                localNLog.Fatal("Failed to update Defendant:\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
                LoggerSQL.Fatal("Failed to update Defendant:\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
            }
        }

        private static void checkLiabilityInformation(
            Entity smallclaiminfo,
            string claimid,
            string assignementid,
            dynamic claimDetails,
            dynamic challengeLiabilityDetails = null)
        {

            if (claimDetails.ToString(Formatting.None).Contains("liability") && claimDetails.ToString(Formatting.None).Contains("liabilityDecision"))
            {
                dynamic liabilityInfo = claimDetails["liability"];

                Console.WriteLine(liabilityInfo);
                localNLog.Info(liabilityInfo);
                if (challengeLiabilityDetails != null) { 
                    Console.WriteLine(challengeLiabilityDetails);
                    localNLog.Info(challengeLiabilityDetails);
                }

                Entity liability = new Entity("nal_smallclaimsliability");

                liability["nal_liabilityresponse"] = getDynamicField<OptionSetValue>(liability, "nal_liabilityresponse", liabilityInfo, "liabilityDecision");
                liability["nal_compensatoradmittingfaultinfullorpart"] = getDynamicField<OptionSetValue>(liability, "nal_compensatoradmittingfaultinfullorpart", liabilityInfo, "admittingFault");
                liability["nal_compensatorliabilityjustification"] = getDynamicField<string>(liability, "nal_compensatorliabilityjustification", liabilityInfo, "liabilityJustification");
                liability["nal_partialliabilityresponsecomments"] = getDynamicField<string>(liability, "nal_partialliabilityresponsecomments", liabilityInfo, "comments");
                liability["nal_capacity"] = getDynamicField<OptionSetValue>(liability, "nal_capacity", liabilityInfo, "capacity");
                liability["nal_doesthedefendantagreewithclaimantseat"] = getDynamicField<OptionSetValue>(liability, "nal_doesthedefendantagreewithclaimantseat", liabilityInfo, "wearingSeatBelt");
                liability["nal_noseatbeltcontributedtotheclaimantinjuries"] = getDynamicField<OptionSetValue>(liability, "nal_noseatbeltcontributedtotheclaimantinjuries", liabilityInfo, "wantToArgue");
                liability["nal_defendantversiontobeprovidedtoexpert"] = getDynamicField<OptionSetValue>(liability, "nal_defendantversiontobeprovidedtoexpert", liabilityInfo, "defendantVersionOfEventsProvidedToMedicalExpert");
                liability["nal_compensatorhasdefendantsversionofevents"] = getDynamicField<OptionSetValue>(liability, "nal_compensatorhasdefendantsversionofevents", liabilityInfo, "defendantVersionOfEvents");
                liability["nal_uploadeddefendantsversionofeventsandsot"] = getDynamicField<OptionSetValue>(liability, "nal_uploadeddefendantsversionofeventsandsot", liabilityInfo, "uploadedDefendantVersionOfEvents");
                liability["nal_availabilityofofficialcourtorder"] = getDynamicField<OptionSetValue>(liability, "nal_availabilityofofficialcourtorder", liabilityInfo, "officialCourtOrder");
                liability["nal_reasonforcausationdispute"] = getDynamicField<string>(liability, "nal_reasonforcausationdispute", liabilityInfo, "reasonForCausationDispute");
                liability["nal_compensatorchangeliabilityafterchallenged"] = getDynamicField<OptionSetValue>(liability, "nal_compensatorchangeliabilityafterchallenged", liabilityInfo, "changeLiabilityResponse");
                liability["nal_commentrefusaltochangeliabilityresponse"] = getDynamicField<string>(liability, "nal_commentrefusaltochangeliabilityresponse", liabilityInfo, "comments");
                liability["nal_liabilityoutcomefromcourtagreement"] = getDynamicField<string>(liability, "nal_liabilityoutcomefromcourtagreement", liabilityInfo, "courtDecisionOnLiability");

                if (challengeLiabilityDetails != null)
                {
                    liability["nal_liabilitypercentageadmittedbythecompensator"] = getDynamicField<Int32>(liability, "nal_liabilitypercentageadmittedbythecompensator", challengeLiabilityDetails["content"], "liabilityPercentageAdmittedByComp");
                }
                else if (liabilityInfo.ToString(Formatting.None).Contains("liabilityPercentage")) 
                { 
                    liability["nal_liabilitypercentageadmittedbythecompensator"] = getDynamicField<Int32>(liability, "nal_liabilitypercentageadmittedbythecompensator", liabilityInfo, "liabilityPercentage");
                }

                liability["nal_claimantcommentondisputedliability"] = getDynamicField<string>(liability, "nal_claimantcommentondisputedliability", liabilityInfo, "claimantCommentOnLiabilityDecision");
                liability["nal_claimantdecisionondisputedliability"] = getDynamicField<OptionSetValue>(liability, "nal_claimantdecisionondisputedliability", liabilityInfo, "claimantDecisionOnLiability");
                liability["nal_justificationfordisagreeingpartialliability"] = getDynamicField<string>(liability, "nal_justificationfordisagreeingpartialliability", liabilityInfo, "disagreeingPartialLiabilityJustification");

                if (liabilityInfo.ToString(Formatting.None).Contains("liabilityHistory"))
                {
                    dynamic liabilityHistory = liabilityInfo["liabilityHistory"];

                    List<List<string>> history = new List<List<string>>();
                    foreach (dynamic entry in liabilityHistory)
                    {
                        string compcomment = "";
                        if (entry.ToString(Formatting.None).Contains("compensatorComment"))
                        {
                            compcomment = entry["compensatorComment"];
                        }

                        string claimcomment = "";
                        if (entry.ToString(Formatting.None).Contains("claimantComment"))
                        {
                            claimcomment = entry["claimantComment"];
                        }

                        string actionedOn = dynDateToString(entry, "actionedOn");
                        string actionedBy = entry["actionedBy"];

                        history.Add(new List<string>() { actionedOn, actionedBy, compcomment, claimcomment });
                    }

                    List<string> latestHistory = history.OrderByDescending(o => o[0]).ToList().FirstOrDefault();

                    liability["nal_actionedon"] = DateTime.Parse(latestHistory[0].ToString());
                    liability["nal_actionedby"] = latestHistory[1];

                    if (latestHistory[2] != "")
                        liability["nal_commentcompensatorwhendisputingliability"] = latestHistory[2];

                    if (latestHistory[3] != "")
                        liability["nal_commentclaimantacceptingchallengingliability"] = latestHistory[3];

                }

                // System Fields
                if (!claimid.Contains("CL"))
                {
                    liability["nal_name"] = claimid + " - Liability Response (" + dynToString(liabilityInfo["liabilityDecision"]) + ")";
                    liability["nal_claimid"] = claimid;
                }
                else
                {
                    string parentClaimId = smallclaiminfo.GetAttributeValue<string>("nal_applicationid");
                    liability["nal_name"] = parentClaimId + " - Liability Response To CL (" + dynToString(liabilityInfo["liabilityDecision"]) + ")";
                    liability["nal_claimid"] = parentClaimId;
                    liability["nal_challengeliabilityclaimid"] = claimid;
                }

                liability["nal_assignmentid"] = assignementid;
                liability["nal_smallclaiminformationid"] = smallclaiminfo.ToEntityReference();
                liability["nal_matterid"] = new EntityReference("incident", smallclaiminfo.GetAttributeValue<EntityReference>("nal_matterid").Id);
                liability["nal_from"] = new OptionSetValue(808850001) /* Other side */;

                // get small claims liability record that matches
                Entity existingLiabilityRecord = helpers.generalQuery("nal_smallclaimsliability", new ColumnSet("nal_smallclaimsliabilityid"), new List<ConditionExpression>() {
                                            new ConditionExpression("nal_actionedon", ConditionOperator.Equal, liability["nal_actionedon"]) /*if actionedon matches*/,
                                            new ConditionExpression("nal_actionedby", ConditionOperator.Equal, liability["nal_actionedby"] /*if actionedby matches*/),
                                            new ConditionExpression("nal_claimid", ConditionOperator.Equal, liability["nal_claimid"] /*if claimid matches*/),
                                            }
                ).Entities.FirstOrDefault();

                if (existingLiabilityRecord == null)
                {
                    try
                    {
                        Guid liabilityid = service.Create(liability);

                        Console.WriteLine("Liability Response created : " + liabilityid.ToString());
                        localNLog.Info("Liability Response created : " + liabilityid.ToString());

                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Failed to create Liability Response:\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
                        localNLog.Fatal("Failed to create Liability Response:\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
                        LoggerSQL.Fatal("Failed to create Liability Response:\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
                    }
                }
                else
                {
                    Console.WriteLine("No new Liability Response found");
                    localNLog.Info("No new Liability Response found");
                }
            }
        }

        private static void createMessageActivity(
            Entity smallclaiminfo,
            string claimid,
            string messageId,
            string createDateTime,
            string category,
            string author,
            string note,
            dynamic smallClaimCalls)
        {

            Entity messageActivity = new Entity("nal_smallclaimsmessage");

            messageActivity["nal_claimid"] = claimid;
            messageActivity["nal_messageid"] = messageId;
            messageActivity["nal_createddatetime"] = DateTime.Parse(createDateTime);
            messageActivity["nal_categorytext"] = category;
            messageActivity["nal_author"] = author;
            messageActivity["description"] = note;
            messageActivity["nal_direction"] = new OptionSetValue(808850000) /* Incoming */;
            messageActivity["scheduledend"] = DateTime.Now;
            messageActivity["subject"] = string.Format("Message received ({0})", category);

            //messageActivity["nal_completeoncreation"] = getDynamicField<OptionSetValue>(, "", , "");

            messageActivity["nal_smallclaiminformationid"] = smallclaiminfo.ToEntityReference();
            messageActivity["regardingobjectid"] = new EntityReference("incident", smallclaiminfo.GetAttributeValue<EntityReference>("nal_matterid").Id);
            
            Entity matter = helpers.returnByIdQuery("incident", new string[]{ "ownerid" }, smallclaiminfo.GetAttributeValue<EntityReference>("nal_matterid").Id);

            messageActivity["ownerid"] = new EntityReference(matter.GetAttributeValue<EntityReference>("ownerid").LogicalName, matter.GetAttributeValue<EntityReference>("ownerid").Id);

            // get small claims message record that matches
            Entity existingMessageActivity = helpers.generalQuery("nal_smallclaimsmessage", new ColumnSet("activityid"), new List<ConditionExpression>() {
                                        new ConditionExpression("nal_messageid", ConditionOperator.Equal, messageId) /*if messageid matches*/,
                                        new ConditionExpression("nal_claimid", ConditionOperator.Equal, claimid /*if claimid matches*/)
                                        }
            ).Entities.FirstOrDefault();

            if (existingMessageActivity == null)
            {
                try
                {
                    Guid messageid = service.Create(messageActivity);

                    Console.WriteLine("Message Activity created : " + messageid.ToString());
                    localNLog.Info("Message Activity created : " + messageid.ToString());

                    createSmallClaimsHistory(
                        smallclaiminfo,
                        claimid,
                        "New Message retrieved successfully (" + category + ")",
                        string.Format("Message Body :\nMessage ID : {0}\nCreated Date Time : {1}\nCategory : {2}\nAuthor : {3}\nNote : {4}",
                            messageId,
                            createDateTime,
                            category,
                            author,
                            note
                        ),
                        "Small Claims Console Logs",
                        smallClaimCalls.APIResponse.ToString(Formatting.None),
                        messageId,
                        createDateTime
                    );

                }
                catch (Exception ex)
                {
                    Console.WriteLine("Failed to create Message Activity:\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
                    localNLog.Fatal("Failed to create Message Activity:\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
                    LoggerSQL.Fatal("Failed to create Message Activity:\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);

                    createSmallClaimsHistory(
                        smallclaiminfo,
                        claimid,
                        "Message failed to retrieve",
                        "Message ID failed " + messageId,
                        "Failed to create Message Activity:\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace,
                        smallClaimCalls.APIResponse.ToString(Formatting.None),
                        null,
                        null
                    );
                }
            }
            else
            {
                Console.WriteLine("No new Message Activity found");
                localNLog.Info("No new Message Activity found");
            }

        }

        private static void updateSmallClaimInformationClaimRemoved(
            Entity smallclaiminfo, 
            string claimid, 
            string claimStatus,
            string removalDate,
            dynamic claimDetails,
            dynamic apiresponse)
        {

            try
            {
                if (smallclaiminfo.GetAttributeValue<string>("nal_smallclaimsstage") != claimStatus)
                {
                    Entity smallclaiminformation = new Entity("nal_smallclaiminformation");
                    smallclaiminformation["nal_smallclaiminformationid"] = smallclaiminfo.Id;
                    smallclaiminformation["nal_smallclaimsstage"] = claimStatus;
                    smallclaiminformation["nal_removedby"] = new OptionSetValue(808850001) /* Other side */;
                    smallclaiminformation["nal_removalmotivation"] = getDynamicField<string>(smallclaiminformation, "nal_removalmotivation", claimDetails, "withdrawalMotivation");
                    smallclaiminformation["nal_removalreason"] = getDynamicField<string>(smallclaiminformation, "nal_removalreason", claimDetails, "withdrawalReason");
                    smallclaiminformation["nal_removaldate"] = DateTime.Parse(removalDate);

                    service.Update(smallclaiminformation);

                    Console.WriteLine("Small Claims Information Updated : SmallClaims ID = {0}", smallclaiminfo.Id);
                    localNLog.Info("Small Claims Information Updated : SmallClaims ID = {0}", smallclaiminfo.Id);

                    createSmallClaimsHistory(
                        smallclaiminfo,
                        claimid,
                        "Successfully processed Portal Removal",
                        string.Format("Claim Status : {0}\nRemoved By: Other Side\nRemoval Reason : {1}\nRemoval Motivation : {2}",
                            claimStatus,
                            getDynamicField<string>(smallclaiminformation, "nal_removalreason", claimDetails, "withdrawalReason"),
                            getDynamicField<string>(smallclaiminformation, "nal_removalmotivation", claimDetails, "withdrawalMotivation")
                        ),
                        "Small Claims Console Logs",
                        apiresponse,
                        null,
                        null
                    );

                }
                else
                {
                    Console.WriteLine("No Update found for Small Claims Information : SmallClaims ID = {0}", smallclaiminfo.Id);
                    localNLog.Info("No Update found for Small Claims Information : SmallClaims ID = {0}", smallclaiminfo.Id);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed to update Small Claims Information:\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
                localNLog.Fatal("Failed to update Small Claims Information:\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
                LoggerSQL.Fatal("Failed to update Small Claims Information:\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
                
                createSmallClaimsHistory(
                    smallclaiminfo,
                    claimid,
                    "Unsuccessfully processed Portal Removal",
                    "Small claims status : " + claimStatus,
                    "Failed to update Small Claims Information:\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace,
                    apiresponse,
                    null,
                    null
                );
            }
        }

        private static void updateSmallClaimInformationSystemAcceptedLiability(
            Entity smallclaiminfo, 
            string claimid)
        {
            try
            {
                Entity smallclaiminformation = new Entity("nal_smallclaiminformation");
                smallclaiminformation["nal_smallclaiminformationid"] = smallclaiminfo.Id;
                smallclaiminformation["nal_liabilityacceptedbyoicsystemtimeout"] = true;

                service.Update(smallclaiminformation);

                Console.WriteLine("OIC System Accepted Liability Updated : SmallClaims ID = {0}", smallclaiminfo.Id);
                localNLog.Info("OIC System Accepted Liability Updated : SmallClaims ID = {0}", smallclaiminfo.Id);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed to update Small Claims Information System AcceptedL iability:\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
                localNLog.Fatal("Failed to update Small Claims Information System Accepted Liability:\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
                LoggerSQL.Fatal("Failed to update Small Claims Information System Accepted Liability:\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
                
                createSmallClaimsHistory(
                    smallclaiminfo,
                    claimid,
                    "Unsuccessfully processed OIC System Accepted Liability",
                    "Failed to update OIC System Accepted Liability on Compensator Timeout.",
                    "Failed to update Small Claims Information:\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace,
                    null,
                    null,
                    null
                );
            }

        }

        private static void updateSmallClaimInformation(
            Entity smallclaiminfo, 
            string claimid, 
            string status,
            dynamic response,
            string apiresponse)
        {

            try
            {
                Entity smallclaiminformation = new Entity("nal_smallclaiminformation");
                smallclaiminformation.Attributes["nal_smallclaiminformationid"] = smallclaiminfo.Id;

                if (smallclaiminfo.GetAttributeValue<string>("nal_smallclaimsstage") != status) 
                {
                    smallclaiminformation["nal_smallclaimsstage"] = status;
                }
                
                if (response.ToString(Formatting.None).Contains("isClaimWithinCluster")) 
                {
                    smallclaiminformation["nal_istheclaimwithinacluster"] = getDynamicField<OptionSetValue>(smallclaiminformation, "nal_istheclaimwithinacluster", response, "isClaimWithinCluster");
                }

                string medId = getMedicalId(response);
                if (medId != smallclaiminfo.GetAttributeValue<string>("nal_medid") && medId != null) 
                { 
                    smallclaiminformation["nal_medid"] = medId;
                }

                if (response.ToString(Formatting.None).Contains("compensatorOrgName"))
                {
                    smallclaiminformation["nal_compensator"] = getDynamicField<string>(smallclaiminformation, "nal_compensator", response, "compensatorOrgName");
                }

                if (smallclaiminformation.Attributes.Count() > 1)
                {

                    service.Update(smallclaiminformation);

                    Console.WriteLine("Small Claims Information Updated : SmallClaims ID = {0}", smallclaiminfo.Id);
                    localNLog.Info("Small Claims Information Updated : SmallClaims ID = {0}", smallclaiminfo.Id);
                    
                    createSmallClaimsHistory(
                        smallclaiminfo,
                        claimid,
                        "Successfully updated with assignement",
                        "Small claims status : " + status,
                        "Small Claims Console Logs",
                        apiresponse,
                        null,
                        null
                    );

                }
                else
                {
                    Console.WriteLine("No Update found for Small Claims Information : SmallClaims ID = {0}", smallclaiminfo.Id);
                    localNLog.Info("No Update found for Small Claims Information : SmallClaims ID = {0}", smallclaiminfo.Id);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed to update Small Claims Information:\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
                localNLog.Fatal("Failed to update Small Claims Information:\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
                LoggerSQL.Fatal("Failed to update Small Claims Information:\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
                
                createSmallClaimsHistory(
                    smallclaiminfo, 
                    claimid,
                    "Unsuccessfully assignement update",
                    "Small claims status : " + status,
                    "Failed to update Small Claims Information:\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace,
                    apiresponse,
                    null,
                    null
                );

            }

        }

        private static void createSmallClaimsHistory(
            Entity smallclaiminfo, 
            string claimid, 
            string nal_name, 
            string nal_details, 
            string nal_applicationtrace,
            string nal_apiresponse,
            string nal_apirecordid,
            string nal_apicreateddatetime)
        {
            try 
            { 
                Entity smallClaimsHistory = new Entity("nal_smallclaimshistory");
                smallClaimsHistory.Attributes["nal_name"] = nal_name;
                smallClaimsHistory.Attributes["nal_smallclaiminformation"] = new EntityReference("nal_smallclaiminformation", smallclaiminfo.Id);
                smallClaimsHistory.Attributes["nal_details"] = nal_details;
                smallClaimsHistory.Attributes["nal_applicationtrace"] = nal_applicationtrace;
                smallClaimsHistory.Attributes["nal_apiresponse"] = nal_apiresponse;
                smallClaimsHistory.Attributes["nal_ingestedfromportal"] = true;

                if (nal_apirecordid != null)
                {
                    smallClaimsHistory.Attributes["nal_apirecordid"] = nal_apirecordid;
                }

                if (nal_apicreateddatetime != null)
                {
                    smallClaimsHistory.Attributes["nal_apicreateddatetime"] = nal_apicreateddatetime;
                }

                service.Create(smallClaimsHistory);
                
                Console.WriteLine("Small Claims History created : Claim ID = {0} : SmallClaims ID = {1}", claimid, smallclaiminfo.Id);
                localNLog.Info("Small Claims History created : Claim ID = {0} : SmallClaims ID = {1}", claimid, smallclaiminfo.Id);
            }
            catch (Exception ex) 
            {
                Console.WriteLine("Failed to create Small Claims History : " + claimid + "\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
                localNLog.Fatal("Failed to create Small Claims History : " + claimid + "\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
                LoggerSQL.Fatal("Failed to create Small Claims History : " + claimid + "\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
            }
        }
    }
}
