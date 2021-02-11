using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using Newtonsoft.Json;
using NLog;

namespace SmallClaimsConsole
{
    class Program
    {
        public static Logger localNLog = LogManager.GetLogger("localLog");

        static IOrganizationService service;

        //Testing vars : 
        static bool IsTesting = bool.Parse(ConfigurationManager.AppSettings.Get("isTesting"));
        static bool ContinueApplication = true;
        static string connectOrg;

        static string dateFrom;// = "20191001T000000.000 GMT"; // string formattedNow = now.ToString("yyyy-MM-ddTHH:mm:ss.fffzzz") ;
        static string timeFrom;// = "10:11:12"; //HH:MM:SS
        static string dateTo;// = "20211001T000000.000 GMT"; // string formattedNow = now.ToString("yyyy-MM-ddTHH:mm:ss.fffzzz") ;

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
                        System.Console.WriteLine("Date not recognised, program will terminate.");
                        ContinueApplication = false;
                    }
                }

            }
            else
            {
                connectOrg = "live";
            }

            #endregion

            if (ContinueApplication) 
            { 
                
                Connect connection = new Connect(connectOrg, localNLog);
                service = connection.ConnectService();
            
                if(service != null)
                {
                    try 
                    { 

                        DynamicHelpers helpers = new DynamicHelpers(localNLog, service);

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
                            }
                        }
                        else
                        {
                            Console.WriteLine("Zettings Error : Failed to retrive Zettings information");
                            localNLog.Fatal("Zettings Error : Failed to retrive Zettings information");
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
                            }
                        }
                        else
                        {
                            Console.WriteLine("API Error : Failed to retrive API settings");
                            localNLog.Fatal("API Error : Failed to retrive API settings");
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

                        #region Get Run DateTime for Assignment and Notification 

                        if (dateFrom == null) 
                        { 
                            try 
                            {
                            
                                EntityCollection lastCreatedHistory = helpers.generalQuery("nal_smallclaimshistory", new ColumnSet("createdon"), new List<ConditionExpression>() { }, new OrderExpression("createdon", OrderType.Descending), true);
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
                            }
                        }
                        #endregion

                        #region Retrieve Assignements and Claim Detail Updates

                        //Retrieve Assignements from API
                        if (smallClaimCalls.ServiceCall("Get Assignments", null, "dateFrom", dateFrom, "timeFrom", timeFrom))
                        {
                            Console.WriteLine(smallClaimCalls.APIResponse);
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
                                    assignval[3] = assignment.createdDateTime;
                                    assignval[4] = assignment.deadlineDateTime;
                                    assignval[5] = assignment.goalDateTime;
                                    assignval[6] = assignment.ToString(Formatting.None);

                                    assignmentslist.Add(assignval.ToList());
                                }

                                foreach (List<string> assignment in assignmentslist)
                                {
                                    // get small claims info from claim ID
                                    string claimid = assignment[0];
                                    Entity smallclaiminfo = helpers.generalQuery("nal_smallclaiminformation", new ColumnSet("nal_smallclaiminformationid", "nal_defendantid"), new List<ConditionExpression>() {
                                        new ConditionExpression("nal_applicationid", ConditionOperator.Equal, claimid) }
                                    ).Entities.FirstOrDefault();

                                    if (smallclaiminfo != null) 
                                    {
                                        // get small claims history record that matches
                                        Entity existingRecords = helpers.generalQuery("nal_smallclaimshistory", new ColumnSet("nal_apirecordid"), new List<ConditionExpression>() {
                                            new ConditionExpression("nal_apirecordid", ConditionOperator.Equal, assignment[1]) /*if assignmentID matches*/,
                                            new ConditionExpression("nal_apicreateddatetime", ConditionOperator.Equal, assignment[3] /*if createdDateTime matches*/)
                                            }
                                        ).Entities.FirstOrDefault();

                                        //if assignement is new
                                        if(existingRecords == null) { 
                                            createSmallClaimsHistory(
                                                smallclaiminfo, 
                                                assignment[0]/*claimID*/, 
                                                "Assignement found : " + assignment[1]/*assignmentID*/,
                                                string.Format(
                                                    "Assignement found for Claim ID : {0}\nAssignement ID : {1}\nAssignement Instructions : {2}\nAssignement Date Created : {3}\nAssignement Date Deadline : {4}\nAssignement Goal Datetime : {5}",
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

                                                /*
                                                 * TO DO :
                                                 * GET CLAIM INFO FROM API AND POPULATE SMALL CLAIMS APPLICABLE FIELDS
                                                 */

                                                updateDefendantRecord(smallclaiminfo, claimid, smallClaimCalls.APIResponse["claimDetails"]);

                                                string claimstatus = smallClaimCalls.APIResponse["claimStatus"];
                                                updateSmallClaimInformation(smallclaiminfo, claimid, claimstatus, smallClaimCalls.APIResponse.ToString(Formatting.None));
                                                
                                            }
                                            else
                                            {
                                                Console.WriteLine(smallClaimCalls.APIResponse);
                                                throw new Exception("SmallClaimAPICalls failed ServiceCall() Method!");
                                            }
                                        }
                                    }
                                    else
                                    {
                                        /*LOG NAHL SQL!!*/
                                        Console.WriteLine("Error : Small Claims not found for Claim ID = {0}", assignment[0]);
                                        localNLog.Warn("Error : Small Claims not found for Claim ID = {0}", assignment[0]);
                                    }
                                }
                            }
                            else
                            {
                                Console.WriteLine("No assignments found on query Get Assignments : from date {0} time {1}", dateFrom, timeFrom);
                                localNLog.Info("No assignments found on query Get Assignments : from date {0} time {1}", dateFrom, timeFrom);
                            }

                        }
                        else
                        {
                            /*LOG Jira Ticket!!*/
                            Console.WriteLine(smallClaimCalls.APIResponse);
                            throw new Exception("SmallClaimAPICalls failed ServiceCall() Method!");
                        }

                        #endregion

                        #region Retrieve Notifications with downstream records (Messages, Documents, (Losses not currently being retrived))

                        if (smallClaimCalls.ServiceCall("Get Notifications", null, "dateFrom", dateFrom, "dateTo", dateTo))
                        {

                            Console.WriteLine(smallClaimCalls.APIResponse);
                            if (smallClaimCalls.APIResponse.ToString(Formatting.None).Contains("notifications"))
                            {

                                List<List<string>> notificationslist = new List<List<string>>();
                                foreach (dynamic notification in smallClaimCalls.APIResponse["notifications"])
                                {
                                    string[] notificationval = new string[4];
                                    notificationval[0] = notification.claimID;
                                    notificationval[1] = notification.NotificationType;
                                    notificationval[2] = notification.createdDateTime;
                                    notificationval[3] = notification.ToString(Formatting.None);

                                    notificationslist.Add(notificationval.ToList());
                                }

                                notificationslist = notificationslist.OrderBy(n => n[0]).ToList();

                                foreach (List<string> notification in notificationslist)
                                {

                                    string claimid = notification[0];
                                    string notificationType = notification[1];
                                    string createdDateTime = notification[2];
                                    string notificationAPIResponse = notification[3];

                                    Entity smallclaiminfo = helpers.generalQuery("nal_smallclaiminformation", new ColumnSet(true), new List<ConditionExpression>() {
                                        new ConditionExpression("nal_applicationid", ConditionOperator.Equal, claimid) }
                                    ).Entities.FirstOrDefault();

                                    if (smallclaiminfo != null)
                                    {

                                        //createSmallClaimsHistory(
                                        //    smallclaiminfo, 
                                        //    claimid,
                                        //    "Notification found : " + claimid,
                                        //    string.Format("Notification found for Claim ID : {0}\nNotification Type : {1}\nCreated Date Time : {2}",
                                        //        claimid,
                                        //        notificationType,
                                        //        createdDateTime
                                        //    ),
                                        //    "Small Claims Console Logs",
                                        //    notificationAPIResponse,
                                        //    claimid,
                                        //    createdDateTime
                                        //);

                                        if (notificationType == "A new message has been added to the claim")
                                        {
                                            //Retrieve Message List
                                            if (smallClaimCalls.ServiceCall("Get Message List", null, "claimID", claimid))
                                            {

                                                Console.WriteLine(smallClaimCalls.APIResponse);

                                                List<List<string>> messagelist = new List<List<string>>();
                                                foreach (dynamic message in smallClaimCalls.APIResponse["results"])
                                                {
                                                    string[] messageval = new string[3];
                                                    messageval[0] = message.id;
                                                    messageval[1] = message.category;
                                                    messageval[2] = message.createDateTime;

                                                    messagelist.Add(messageval.ToList());
                                                }

                                                EntityCollection existingRecords = helpers.generalQuery("nal_smallclaimshistory", new ColumnSet("nal_apirecordid"), new List<ConditionExpression>() {
                                                    new ConditionExpression("nal_apirecordid", ConditionOperator.In, messagelist.Select(m=>m[0]).ToArray()) }
                                                );

                                                int newMessageCount = (messagelist.Count() - existingRecords.Entities.Count());
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
                                                        if (existingRecords.Entities.Where(e => e.GetAttributeValue<string>("nal_apirecordid") == message[0]).Count() == 0)
                                                        {
                                                            if (smallClaimCalls.ServiceCall("Get Message", null, "messageID", message[0]))
                                                            {
                                                                Console.WriteLine(smallClaimCalls.APIResponse);

                                                                var messageInfo = smallClaimCalls.APIResponse["messageInfo"];
                                                                string author = smallClaimCalls.APIResponse["author"];
                                                                string note = smallClaimCalls.APIResponse["note"];

                                                                string category = messageInfo.category;
                                                                string createDateTime = messageInfo.createDateTime;
                                                                string messageId = messageInfo.id;

                                                                createSmallClaimsHistory(
                                                                    smallclaiminfo,
                                                                    claimid,
                                                                    "New Message retrieved successfully",
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
                                                            else
                                                            {
                                                                Console.WriteLine(smallClaimCalls.APIResponse);
                                                                throw new Exception("SmallClaimAPICalls failed ServiceCall() Method!");
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            else
                                            {
                                                Console.WriteLine(smallClaimCalls.APIResponse);
                                                throw new Exception("SmallClaimAPICalls failed ServiceCall() Method!");
                                            }

                                        }

                                        if (notificationType == "A new document has been added to the claim")
                                        {
                                            //Retrieve Attachement List
                                            if (smallClaimCalls.ServiceCall("Get Attachment List", null, "claimID", claimid))
                                            {
                                                Console.WriteLine(smallClaimCalls.APIResponse);

                                                List<List<string>> doclist = new List<List<string>>();
                                                foreach (dynamic doc in smallClaimCalls.APIResponse["results"])
                                                {
                                                    string[] docval = new string[6];
                                                    docval[0] = doc.id;
                                                    docval[1] = doc.category;
                                                    docval[2] = doc.fileName;
                                                    docval[3] = doc.memo;
                                                    docval[4] = doc.createDateTime;
                                                    docval[5] = doc.ToString(Formatting.None);

                                                    doclist.Add(docval.ToList());
                                                }

                                                EntityCollection existingRecords = helpers.generalQuery("nal_smallclaimshistory", new ColumnSet("nal_apirecordid"), new List<ConditionExpression>() {
                                                    new ConditionExpression("nal_apirecordid", ConditionOperator.In, doclist.Select(m=>m[0]).ToArray()) }
                                                );

                                                int newDocsCount = (doclist.Count() - existingRecords.Entities.Count());
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
                                                                Console.WriteLine(smallClaimCalls.APIResponse);

                                                                dynamic document = smallClaimCalls.APIResponse["fileInfo"];
                                                                string filestream = smallClaimCalls.APIResponse["fileStream"];

                                                                string subject = document.category;
                                                                string filename = document.fileName;
                                                                string created = document.createdDateTime;
                                                                string memo = document.memo;
                                                                string docid = document.id;
                                                                string docbytes = filestream;
                                                                string mimetype = filename.Substring(filename.LastIndexOf(".") + 1);

                                                                try
                                                                {
                                                                    SharepointDocuments docProcessor = new SharepointDocuments(service, helpers);
                                                                    docProcessor.CreateDocumentProcessor(smallclaiminfo.GetAttributeValue<EntityReference>("nal_matterid"), subject, filename, docbytes, mimetype);

                                                                    createSmallClaimsHistory(
                                                                        smallclaiminfo,
                                                                        claimid,
                                                                        "New document retrieved successfully : " + docid,
                                                                        string.Format("Document File name : {0}\nDocument Subject : {1}\nCreated Date Time : {2}\nMemo : {3}\nDocumant ID : {4}",
                                                                            filename,
                                                                            subject,
                                                                            created,
                                                                            memo,
                                                                            docid
                                                                        ),
                                                                        "Small Claims Console Logs",
                                                                        smallClaimCalls.APIResponse.ToString(Formatting.None),
                                                                        docid,
                                                                        created
                                                                    );

                                                                }
                                                                catch (Exception ex)
                                                                {
                                                                    createSmallClaimsHistory(
                                                                        smallclaiminfo,
                                                                        claimid,
                                                                        "Failed to return document : " + docid,
                                                                        string.Format("Document File name : {0}\nDocument Subject : {1}\nCreated Date Time : {2}\nMemo : {3}\nDocumant ID : {4}",
                                                                            filename,
                                                                            subject,
                                                                            created,
                                                                            memo,
                                                                            docid
                                                                        ),
                                                                        "Failed to return document:\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace,
                                                                        smallClaimCalls.APIResponse.ToString(Formatting.None),
                                                                        null,
                                                                        null
                                                                    );

                                                                }
                                                            }
                                                            else
                                                            {
                                                                Console.WriteLine(smallClaimCalls.APIResponse);
                                                                throw new Exception("SmallClaimAPICalls failed ServiceCall() Method!");
                                                            }
                                                        }
                                                    }
                                                }

                                            }
                                            else
                                            {
                                                Console.WriteLine(smallClaimCalls.APIResponse);
                                                throw new Exception("SmallClaimAPICalls failed ServiceCall() Method!");
                                            }

                                        }

                                        if (notificationType == "New loss(es) have been added to the claim")
                                        {
                                            //Retrieve Claim details
                                            if (smallClaimCalls.ServiceCall("Get Claim Details", null, "claimID", claimid))
                                            {
                                                Console.WriteLine(smallClaimCalls.APIResponse);

                                                dynamic claimstatus = smallClaimCalls.APIResponse["claimStatus"];

                                                updateSmallClaimInformation(smallclaiminfo, claimid, claimstatus.ToString(Formatting.None), smallClaimCalls.APIResponse.ToString(Formatting.None));

                                            }
                                            else
                                            {
                                                Console.WriteLine(smallClaimCalls.APIResponse);
                                                throw new Exception("SmallClaimAPICalls failed ServiceCall() Method!");
                                            }

                                        }

                                        /*
                                         * TO DO :
                                         * DEPENDING ON NOTIFICATION TYPE GET APPROPRIATE TYPE AND POPULATE DYNAMICS
                                         *      -> Messages
                                         *      -> Docs
                                         *      -> Losses
                                         */
                                    }
                                    else
                                    {
                                        Console.WriteLine("Warn : Small Claims not found for Claim ID = {0} :: Notification = {1}", claimid, notificationType);
                                        localNLog.Warn("Warn : Small Claims not found for Claim ID = {0} :: Notification = {1}", claimid, notificationType);
                                    }
                                }
                            }
                            else
                            {
                                Console.WriteLine("No notification found on query Get Notifications : from {0} to {1}", dateFrom, dateTo);
                                localNLog.Info("No notification found on query Get Notifications : from {0} to {1}", dateFrom, dateTo);
                            }
                        }
                        else
                        {
                            /*LOG Jira Ticket!!*/
                            Console.WriteLine(smallClaimCalls.APIResponse);
                            throw new Exception("SmallClaimAPICalls failed ServiceCall() Method!");
                        }

                        #endregion
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Main Body : Exception Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
                        localNLog.Fatal("Main Body : Exception Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
                    }
                }
                else
                {
                    /*Jira Ticket*/
                    Console.WriteLine("Unable to connect to CRM Dynamics Org : " + connectOrg);
                    localNLog.Fatal("Unable to connect to CRM Dynamics Org : " + connectOrg);
                }

            }

            Console.WriteLine("Program has ended press any key to terminate");
            Console.ReadKey();

        }

        private static void updateDefendantRecord(Entity smallclaiminfo, string claimid, dynamic claimDetails)
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

                    Entity defendant = new Entity("pi_defendant");
                    defendant.Attributes["pi_defendantid"] = defendantreference.Id;
                    defendant.Attributes["pi_referencenumber"] = claimReferenceNumber;
                    defendant.Attributes["pi_contactname"] = claimHandlerName;
                    defendant.Attributes["pi_telephonenumber"] = claimHandlerPhone;
                    defendant.Attributes["pi_emailaddress"] = claimHandlerEmail;

                    service.Update(defendant);

                    Console.WriteLine("Defendant Updated : " + defendantreference.Id);
                    localNLog.Info("Defendant Updated : " + defendantreference.Id);
                }
                else
                {
                    Console.WriteLine("Defendant not found in Small Claims : " + smallclaiminfo.Id);
                    localNLog.Warn("Defendant not found in Small Claims : " + smallclaiminfo.Id);
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine("Failed to update Defendant:\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
                localNLog.Fatal("Failed to update Defendant:\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
            }
        }

        private static void updateSmallClaimInformation(
            Entity smallclaiminfo, 
            string claimid, 
            string status, 
            string apiresponse)
        {

            try
            {
                Entity smallclaiminformation = new Entity("nal_smallclaiminformation");
                smallclaiminformation.Attributes["nal_smallclaiminformationid"] = smallclaiminfo.Id;
                smallclaiminformation.Attributes["nal_smallclaimsstage"] = status;

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
            catch (Exception ex)
            {
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

                Console.WriteLine("Failed to update Small Claims Information:\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
                localNLog.Fatal("Failed to update Small Claims Information:\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
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

                if(nal_apirecordid != null)
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
                //Jira ticket
                Console.WriteLine("Failed to create Small Claims History : " + claimid + "\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
                localNLog.Fatal("Failed to create Small Claims History : " + claimid + "\nException Error : " + ex.Message + "\nError Stack : " + ex.StackTrace);
            }
        }
    }
}
