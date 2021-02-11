using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Runtime.Serialization.Json;
using NLog;

//D365 Assembly References
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Tooling.Connector;

namespace SmallClaimsConsole
{
    public class DynamicHelpers
    {
        Logger _tracingService = null;
        IOrganizationService _service = null;

        public DynamicHelpers(Logger tracingService, IOrganizationService service)
        {
            _tracingService = tracingService;
            _service = service;
            _tracingService.Info("Plugin Helpers Instantiated");
        }

        public Entity retrieveEntity(EntityReference entityToRetrieve, ColumnSet cols)
        {
            Entity ret = null;

            try
            {
                RetrieveRequest targetEntity = new RetrieveRequest();
                targetEntity.ColumnSet = cols;
                targetEntity.Target = entityToRetrieve;
                ret = (Entity)((RetrieveResponse)_service.Execute(targetEntity)).Entity;
            }
            catch (Exception ex)
            {
                _tracingService.Warn("PluginHelpers.standardRetrieveEntity Failed : " + ex.Message);
            }

            return ret;
        }

        public EntityMetadata retrieveEntityFieldMetaData(string entityLogicalName)
        {
            EntityMetadata ret = null;

            try
            {
                RetrieveEntityRequest req = new RetrieveEntityRequest();
                req.LogicalName = entityLogicalName;
                req.EntityFilters = EntityFilters.Attributes;
                req.RetrieveAsIfPublished = true;

                RetrieveEntityResponse resp = _service.Execute(req) as RetrieveEntityResponse;
                if (resp != null)
                {
                    ret = resp.EntityMetadata;
                }
            }
            catch (Exception ex)
            {
                _tracingService.Warn("Error in PluginHelpers.retrieveEntityFieldMetaData : " + ex.Message);
            }


            return ret;
        }

        public AttributeMetadata retrieveAttribMetaData(string entitylogicalname, string attributeLogicalName)
        {
            AttributeMetadata ret = null;

            try
            {
                RetrieveAttributeRequest req = new RetrieveAttributeRequest();
                req.EntityLogicalName = entitylogicalname;
                req.LogicalName = attributeLogicalName;
                req.RetrieveAsIfPublished = true;

                RetrieveAttributeResponse resp = _service.Execute(req) as RetrieveAttributeResponse;
                if (resp != null)
                {
                    ret = resp.AttributeMetadata;
                }

            }
            catch (Exception ex)
            {
                _tracingService.Warn("Error in PluginHelpers.retrieveAttributeMetaData : " + ex.Message);
            }

            return ret;
        }

        public Entity returnByIdQuery(string entityLogicalName, string[] colList, Guid Id)
        {
            Entity e = new Entity();
            ColumnSet cs = new ColumnSet(true);

            if (colList[0] != "True")
            {
                cs = new ColumnSet(colList);
            }

            //active only
            //qe.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));

            try
            {
                e = _service.Retrieve(entityLogicalName, Id, cs);
            }
            catch (Exception ex)
            {
                _tracingService.Warn("PluginHelpers.returnByIdQuery Failed : " + ex.Message);
            }

            return e;
        }

        public EntityCollection generalQuery(string entityLogicalName, ColumnSet cols)
        {
            EntityCollection ec = new EntityCollection();
            QueryExpression qe = new QueryExpression(entityLogicalName);
            qe.ColumnSet = cols;

            //active only
            qe.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));

            try
            {
                ec = _service.RetrieveMultiple(qe);
            }
            catch (Exception ex)
            {
                _tracingService.Warn("PluginHelpers.retrieveEntity Failed : " + ex.Message);
            }

            return ec;
        }

        public EntityCollection generalQuery(string entityLogicalName, ColumnSet cols, List<ConditionExpression> lstConditions)
        {
            EntityCollection ec = new EntityCollection();
            QueryExpression qe = new QueryExpression(entityLogicalName);
            qe.ColumnSet = cols;
            foreach (ConditionExpression condEx in lstConditions)
            {
                qe.Criteria.AddCondition(condEx);
            }
            qe.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));

            try
            {
                ec = _service.RetrieveMultiple(qe);
            }
            catch (Exception ex)
            {
                _tracingService.Warn("PluginsHelpers.generalQuery(entitylogicalname,cols,lstConditions) Failed : " + ex.Message);
            }

            return ec;
        }

        public EntityCollection generalQuery(string entityLogicalName, ColumnSet cols, List<ConditionExpression> lstConditions, Int32 statecode)
        {
            EntityCollection ec = new EntityCollection();
            QueryExpression qe = new QueryExpression(entityLogicalName);
            qe.ColumnSet = cols;
            foreach (ConditionExpression condEx in lstConditions)
            {
                qe.Criteria.AddCondition(condEx);
            }
            qe.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, statecode));

            try
            {
                ec = _service.RetrieveMultiple(qe);
            }
            catch (Exception ex)
            {
                _tracingService.Warn("PluginsHelpers.generalQuery(entityLogicalName, cols, lstConditions, statecode) Failed : " + ex.Message);
            }

            return ec;
        }

        public EntityCollection generalQuery(string entityLogicalName, ColumnSet cols, List<ConditionExpression> lstConditions, bool includeState)
        {
            EntityCollection ec = new EntityCollection();
            QueryExpression qe = new QueryExpression(entityLogicalName);
            qe.ColumnSet = cols;
            foreach (ConditionExpression condEx in lstConditions)
            {
                qe.Criteria.AddCondition(condEx);
            }
            if (includeState == true)
            {
                qe.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
            }

            try
            {
                ec = _service.RetrieveMultiple(qe);
            }
            catch (Exception ex)
            {
                _tracingService.Warn("PluginsHelpers.generalQuery(entityLogicalName, cols, lstConditions, includeState) Failed : " + ex.Message);
            }


            return ec;
        }

        public EntityCollection generalQuery(string entityLogicalName, ColumnSet cols, List<ConditionExpression> lstConditions, OrderExpression order, bool takeTop1)
        {
            EntityCollection ec = new EntityCollection();
            QueryExpression qe = new QueryExpression(entityLogicalName);
            qe.ColumnSet = cols;
            foreach (ConditionExpression condEx in lstConditions)
            {
                qe.Criteria.AddCondition(condEx);
            }
            qe.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
            qe.Orders.Add(order);

            if (takeTop1)
            {
                PagingInfo pageInfo = new PagingInfo();
                pageInfo.Count = 1;
                pageInfo.PageNumber = 1;

                qe.PageInfo = pageInfo;
            }

            try
            {
                ec = _service.RetrieveMultiple(qe);
            }
            catch (Exception ex)
            {
                _tracingService.Warn("PluginsHelpers.generalQuery(entityLogicalName, cols, lstConditions, order, takeTop1) Failed : " + ex.Message);
            }


            return ec;
        }

        public EntityCollection generalQuery(string entityLogicalName, bool allCols, List<ConditionExpression> lstConditions, OrderExpression order, bool takeTop1)
        {
            EntityCollection ec = new EntityCollection();
            QueryExpression qe = new QueryExpression(entityLogicalName);
            qe.ColumnSet = new ColumnSet(allCols);
            foreach (ConditionExpression condEx in lstConditions)
            {
                qe.Criteria.AddCondition(condEx);
            }
            qe.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
            qe.Orders.Add(order);

            if (takeTop1)
            {
                PagingInfo pageInfo = new PagingInfo();
                pageInfo.Count = 1;
                pageInfo.PageNumber = 1;

                qe.PageInfo = pageInfo;
            }

            try
            {
                ec = _service.RetrieveMultiple(qe);
            }
            catch (Exception ex)
            {
                _tracingService.Warn("PluginsHelpers.generalQuery(entityLogicalName, allCols, lstConditions, order, takeTop1) Failed : " + ex.Message);

            }


            return ec;
        }

        public EntityCollection generalQuery(string entityLogicalName, bool allCols, List<ConditionExpression> lstConditions, bool includeState)
        {
            EntityCollection ec = new EntityCollection();
            QueryExpression qe = new QueryExpression(entityLogicalName);
            qe.ColumnSet = new ColumnSet(allCols);
            foreach (ConditionExpression condEx in lstConditions)
            {
                qe.Criteria.AddCondition(condEx);
            }
            if (includeState == true)
            {
                qe.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
            }

            try
            {
                ec = _service.RetrieveMultiple(qe);
            }
            catch (Exception ex)
            {
                _tracingService.Warn("PluginsHelpers.generalQuery(entityLogicalName, allCols, lstConditions, includeState) Failed : " + ex.Message);
            }


            return ec;
        }

        public EntityCollection generalQuery(string entityLogicalName, bool allCols, List<ConditionExpression> lstConditions)
        {
            EntityCollection ec = new EntityCollection();
            QueryExpression qe = new QueryExpression(entityLogicalName);
            qe.ColumnSet = new ColumnSet(allCols);
            foreach (ConditionExpression condEx in lstConditions)
            {
                qe.Criteria.AddCondition(condEx);
            }
            qe.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));

            try
            {
                ec = _service.RetrieveMultiple(qe);
            }
            catch (Exception ex)
            {
                _tracingService.Warn("PluginsHelpers.generalQuery(entityLogicalName, allCols, lstConditions) Failed : " + ex.Message);

            }


            return ec;
        }
        public EntityCollection generalQuery(string entityLogicalName, ColumnSet cols, List<ConditionExpression> lstConditions, List<LinkEntity> lstLinkEntities, OrderExpression order, bool takeTop1)
        {
            EntityCollection ec = new EntityCollection();
            QueryExpression qe = new QueryExpression(entityLogicalName);
            qe.ColumnSet = cols;
            foreach (ConditionExpression condEx in lstConditions)
            {
                qe.Criteria.AddCondition(condEx);
            }
            foreach (LinkEntity linkEnt in lstLinkEntities)
            {
                qe.LinkEntities.Add(linkEnt);
            }
            qe.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
            qe.Orders.Add(order);

            if (takeTop1)
            {
                PagingInfo pageInfo = new PagingInfo();
                pageInfo.Count = 1;
                pageInfo.PageNumber = 1;

                qe.PageInfo = pageInfo;
            }

            try
            {
                ec = _service.RetrieveMultiple(qe);
            }
            catch (Exception ex)
            {
                _tracingService.Warn("PluginsHelpers.generalQuery(string entityLogicalName, ColumnSet cols, List<ConditionExpression> lstConditions, List<LinkEntity> lstLinkEntities, OrderExpression order, bool takeTop1) Failed : " + ex.Message);
            }


            return ec;
        }


        public EntityCollection generalQuery(string FetchXML)
        {
            EntityCollection ret = new EntityCollection();
            RetrieveMultipleRequest fetchRequest = new RetrieveMultipleRequest
            {
                Query = new FetchExpression(FetchXML)
            };

            try
            {
                ret = ((RetrieveMultipleResponse)_service.Execute(fetchRequest)).EntityCollection;
            }
            catch (Exception ex)
            {
                _tracingService.Warn("PluginsHelpers.generalQuery(fetchXml) Failed : " + ex.Message);
            }


            return ret;
        }

        public bool SetStateRequest(EntityReference entityRef, int statecode, int statuscode)
        {
            bool ret = false;
            SetStateRequest setState = new SetStateRequest();
            setState.EntityMoniker = new EntityReference(entityRef.LogicalName, entityRef.Id);
            setState.State = new OptionSetValue(statecode);
            setState.Status = new OptionSetValue(statuscode);

            try
            {
                SetStateResponse resp = (SetStateResponse)_service.Execute(setState);
                if (resp != null)
                {
                    ret = true;
                }
            }
            catch (Exception ex)
            {
                _tracingService.Warn("PluginHelpers.SetStateRequest Failed : " + ex.Message);
            }

            return ret;
        }

        public string getEntityName(string entityLogicalName, string attributeName, Guid id)
        {
            string ret = string.Empty;
            try
            {
                Entity toGet = _service.Retrieve(entityLogicalName, id, new ColumnSet(new string[] { attributeName }));
                if (toGet != null)
                {
                    ret = toGet.GetAttributeValue<string>(attributeName);
                    _tracingService.Info("Helpers : getEntityName : " + ret);
                }
            }
            catch (Exception ex)
            {
                _tracingService.Warn("PluginHelpers.getEntityName Failed : " + ex.Message);
            }


            return ret;
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

        public string SerializeJson(object myobject)
        {

            DataContractJsonSerializer js = new DataContractJsonSerializer(myobject.GetType());
            MemoryStream msObj = new MemoryStream();
            js.WriteObject(msObj, myobject);
            msObj.Position = 0;
            StreamReader sr = new StreamReader(msObj);

            string json = sr.ReadToEnd();

            sr.Close();
            msObj.Close();

            return json;
        }

        public string RetrieveZettingsPayload(string specificIdentityKey)
        {
            string ret = string.Empty;

            try
            {
                if (!string.IsNullOrEmpty(specificIdentityKey))
                {
                    QueryExpression qe = new QueryExpression("nal_zettings");
                    qe.ColumnSet = new ColumnSet(true);

                    qe.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0)); //Active
                    qe.Criteria.AddCondition(new ConditionExpression("nal_specificidentitykey", ConditionOperator.Equal, specificIdentityKey));

                    EntityCollection ec = _service.RetrieveMultiple(qe);

                    if (ec.Entities.Count == 1)
                    {
                        foreach (Entity e in ec.Entities)
                        {
                            ret = e.GetAttributeValue<string>("nal_payload");
                        }
                    }
                    else
                    {
                        _tracingService.Warn("Helpers.RetrieveZettingsPayload found more than one matching Zettings record for the Specific Identity Key (" + specificIdentityKey + ") Check configurations. You can only have ONE record with this KEY");
                    }
                }
            }
            catch (Exception ex)
            {
                _tracingService.Warn("Fatal Error in Helpers.RetrieveZettingsPayload : " + ex.Message);
                throw new InvalidPluginExecutionException("Unable to retrieve Zettings entity payload. Check Zettings entity :" + ex.Message);
            }

            return ret;
        }

        public string GetOptionSetLabelFromValue(string entityLogicalName, string attribute, OptionSetValue option)
        {
            string optionLabel = String.Empty;

            RetrieveAttributeRequest attributeRequest = new RetrieveAttributeRequest
            {
                EntityLogicalName = entityLogicalName,
                LogicalName = attribute,
                RetrieveAsIfPublished = true
            };

            RetrieveAttributeResponse attributeResponse = (RetrieveAttributeResponse)_service.Execute(attributeRequest);
            AttributeMetadata attrMetadata = (AttributeMetadata)attributeResponse.AttributeMetadata;
            PicklistAttributeMetadata picklistMetadata = (PicklistAttributeMetadata)attrMetadata;

            // For every value within all optionsetvalues
            //  (all of the values in the drop down list)
            foreach (OptionMetadata optionMeta in
                picklistMetadata.OptionSet.Options)
            {
                // Check to see if our current value matches
                if (optionMeta.Value == option.Value)
                {
                    // If our numeric value matches, set the string to our status code
                    //  label
                    optionLabel = optionMeta.Label.UserLocalizedLabel.Label;
                }
            }

            return optionLabel;
        }

        public int GetOptionSetValueFromLabel(string entityLogicalName, string attribute, string optionLabel)
        {
            int optionsetValue = -1;

            RetrieveAttributeRequest attributeRequest = new RetrieveAttributeRequest
            {
                EntityLogicalName = entityLogicalName,
                LogicalName = attribute,
                RetrieveAsIfPublished = true
            };

            RetrieveAttributeResponse attributeResponse = (RetrieveAttributeResponse)_service.Execute(attributeRequest);
            AttributeMetadata attrMetadata = (AttributeMetadata)attributeResponse.AttributeMetadata;
            PicklistAttributeMetadata picklistMetadata = (PicklistAttributeMetadata)attrMetadata;

            foreach (OptionMetadata optionMeta in picklistMetadata.OptionSet.Options)
            {
                if (optionMeta.Label.UserLocalizedLabel.Label == optionLabel)
                {
                    optionsetValue = optionMeta.Value.Value;
                }
            }

            return optionsetValue;
        }

        public decimal getMoneyFieldValue(Entity e, string field)
        {
            decimal ret = 0.00M;
            if (e.Attributes.Contains(field))
            {
                decimal gotMoney = e.GetAttributeValue<Money>(field).Value;
                if (gotMoney != 0.00M)
                {
                    //ret = Math.Round(gotMoney,2);
                    ret = decimal.Round(gotMoney, 2, MidpointRounding.AwayFromZero);
                }
            }
            return ret;
        }

        public decimal getDecimalFieldValue(Entity e, string field)
        {
            decimal ret = 0.00M;

            if (e.Attributes.Contains(field))
            {
                decimal gotDecimal = e.GetAttributeValue<Decimal>(field);
                if (gotDecimal != 0.00M)
                {
                    ret = decimal.Round(gotDecimal, 2, MidpointRounding.AwayFromZero);
                }
            }
            return ret;

        }

        public bool getBoolFieldValue(Entity e, string field)
        {
            bool ret = false;

            if (e.Attributes.Contains(field))
            {
                ret = e.GetAttributeValue<Boolean>(field);
            }

            return ret;
        }

        public Int32 getIntegerFieldValue(Entity e, string field)
        {
            Int32 ret = 0;
            if (e.Attributes.Contains(field))
            {
                ret = e.GetAttributeValue<Int32>(field);
            }
            return ret;
        }

        public string getStringFieldValue(Entity e, string field)
        {
            string ret = string.Empty;
            if (e.Attributes.Contains(field))
            {
                ret = e.GetAttributeValue<String>(field);
            }
            return ret;
        }

        public EntityReference getLookupFieldValue(Entity e, string field)
        {
            EntityReference ret = new EntityReference();

            if (e.Attributes.Contains(field))
            {
                ret = e.GetAttributeValue<EntityReference>(field);
            }

            return ret;
        }

        public decimal roundDecimal(Decimal value)
        {
            decimal ret = 0.00M;
            ret = decimal.Round(value, 2, MidpointRounding.AwayFromZero);
            return ret;
        }

        public void MultiExecution(EntityCollection entities, string requestType)
        {
            ExecuteMultipleRequest requestWithResults = null;
            int batchCount = 0;

            foreach (Entity ent in entities.Entities)
            {
                if (requestWithResults == null)
                {
                    requestWithResults = new ExecuteMultipleRequest()
                    {
                        // Assign settings that define execution behavior: continue on error, return responses. 
                        Settings = new ExecuteMultipleSettings()
                        {
                            ContinueOnError = true,
                            ReturnResponses = true
                        },
                        // Create an empty organization request collection.
                        Requests = new OrganizationRequestCollection()
                    };
                }

                if (requestType == "UpdateRequest")
                {
                    UpdateRequest updateRequest = new UpdateRequest { Target = ent };
                    requestWithResults.Requests.Add(updateRequest);
                }
                else if (requestType == "CreateRequest")
                {
                    CreateRequest createRequest = new CreateRequest { Target = ent };
                    requestWithResults.Requests.Add(createRequest);
                }

                if (requestWithResults.Requests.Count() == 1000 || requestWithResults.Requests.Count() + (1000 * batchCount) == entities.Entities.Count)
                {
                    batchCount++;
                    try
                    {
                        // Execute all the requests in the request collection using a single web method call.
                        ExecuteMultipleResponse responseWithResults = (ExecuteMultipleResponse)_service.Execute(requestWithResults);

                        string valids = "";
                        string errors = "";
                        // Display the results returned in the responses.
                        foreach (var responseItem in responseWithResults.Responses)
                        {
                            // A valid response.
                            if (responseItem.Response != null)
                            {
                                valids += String.Format("Valid : {0} - {1}\n", requestWithResults.Requests[responseItem.RequestIndex], responseItem.RequestIndex);
                            }
                            // An error has occurred.
                            else if (responseItem.Fault != null)
                            {
                                errors += String.Format("Error : {0} - {1} - {2}\n", requestWithResults.Requests[responseItem.RequestIndex], responseItem.RequestIndex, responseItem.Fault);
                            }
                        }

                        if (errors.Length > 0)
                        {
                            _tracingService.Warn("Helpers.MultiExecution - Completed with Errors => " /*\nValid :: \n" + valids */ + "Errors :: \n" + errors);
                        }

                        _tracingService.Warn("Completed batch n. " + batchCount + " number of records changed = " + requestWithResults.Requests.Count);
                    }
                    catch (Exception e)
                    {
                        _tracingService.Warn("ExecuteMultipleResponse Error => " + e.Message);
                    }
                    finally
                    {
                        requestWithResults = null;
                    }

                }
            }

        }

        /// <summary>
        /// Basic execute workflow call
        /// </summary>
        /// <param name="service"></param>
        /// <param name="workflowid"></param>
        /// <param name="entityid"></param>
        /// <returns></returns>
        public bool ExecuteWorkflow(IOrganizationService service, Guid workflowid, Guid entityid)
        {
            bool ret = true;
            try
            {
                ExecuteWorkflowRequest request = new ExecuteWorkflowRequest()
                {
                    WorkflowId = workflowid,
                    EntityId = entityid
                };

                ExecuteWorkflowResponse response = (ExecuteWorkflowResponse)service.Execute(request);

            }
            catch (Exception ex)
            {
                _tracingService.Warn("Error in Helpers.ExecuteWorkflow : " + ex.Message);
            }


            return ret;
        }

        /// <summary>
        /// Return entity values from single entry point
        /// </summary>
        /// <param name="entityToQuery"></param>
        /// <param name="queryValue"></param>
        /// <param name="paramlist">First param should be calling downstream method</param>
        /// <returns>T entity value</returns>
        public T getEntityValue<T>(Entity entityToQuery, string queryValue, params string[] paramlist)
        {
            if (entityToQuery == null)
                throw new ArgumentException("Unable to retrieve value as no entity provided", "entityToQuery");

            if (String.IsNullOrWhiteSpace(queryValue))
                throw new ArgumentException("Please specify the value you wish to retrieve", "queryValue");

            if (paramlist != null && paramlist.Count() > 0 && entityToQuery.Contains(queryValue))
            {
                switch (paramlist[0])
                {
                    case ("GetOptionSetLabelFromValue"):
                        return (T)Convert.ChangeType(GetOptionSetLabelFromValue(paramlist[1], paramlist[2], getTAttributeValue<OptionSetValue>(entityToQuery.Attributes[queryValue])), typeof(T));
                    default:
                        throw new ArgumentException("Downstream Method not defined!", "paramlist[0]");
                }
            }

            T result = default(T);

            try
            {
                if (entityToQuery.Contains(queryValue))
                {
                    result = (T)entityToQuery[queryValue];
                }
            }
            catch (Exception)
            {
                if (typeof(T) == typeof(string))
                {
                    return (T)Convert.ChangeType(getAttributeValue(entityToQuery[queryValue]), typeof(T));
                }
                else if (typeof(T) == typeof(DateTime))
                {
                    return (T)(object)DateTime.Parse(getAttributeValue(entityToQuery[queryValue]));
                }
                else
                {
                    result = (T)(object)getTAttributeValue<T>(entityToQuery[queryValue]);
                    if (result != null)
                        return result;
                }

                result = default(T);
            }

            return result;
        }

        public T getTAttributeValue<T>(object p)
        {
            if (typeof(T).ToString() == "Microsoft.Xrm.Sdk.EntityReference" && p.ToString() == "Microsoft.Xrm.Sdk.EntityReference")
            {
                return (T)(object)((EntityReference)p);
            }
            if (typeof(T).ToString() == "Microsoft.Xrm.Sdk.OptionSetValue" && p.ToString() == "Microsoft.Xrm.Sdk.OptionSetValue")
            {
                return (T)(object)((OptionSetValue)p);
            }
            if (typeof(T).ToString() == "Microsoft.Xrm.Sdk.Money" && p.ToString() == "Microsoft.Xrm.Sdk.Money")
            {
                return (T)(object)((Money)p);
            }
            if (p.ToString() == "Microsoft.Xrm.Sdk.AliasedValue")
            {
                string ptemp = ((Microsoft.Xrm.Sdk.AliasedValue)p).Value.ToString();

                if (new List<string> { "Microsoft.Xrm.Sdk.EntityReference", "Microsoft.Xrm.Sdk.OptionSetValue", "Microsoft.Xrm.Sdk.Money", "Microsoft.Xrm.Sdk.AliasedValue" }.Contains(ptemp))
                {
                    return (T)(object)getTAttributeValue<T>(((Microsoft.Xrm.Sdk.AliasedValue)p).Value);
                }

                return (T)(object)((Microsoft.Xrm.Sdk.AliasedValue)p);
            }
            else
            {
                return (T)(object)p.ToString();
            }
        }

        public string getAttributeValue(object p)
        {
            if (p.ToString() == "Microsoft.Xrm.Sdk.EntityReference")
            {
                return ((EntityReference)p).Id.ToString();
            }
            if (p.ToString() == "Microsoft.Xrm.Sdk.OptionSetValue")
            {
                return ((OptionSetValue)p).Value.ToString();
            }
            if (p.ToString() == "Microsoft.Xrm.Sdk.Money")
            {
                return ((Money)p).Value.ToString();
            }
            if (p.ToString() == "Microsoft.Xrm.Sdk.AliasedValue")
            {
                string ptemp = ((Microsoft.Xrm.Sdk.AliasedValue)p).Value.ToString();

                if (new List<string> { "Microsoft.Xrm.Sdk.EntityReference", "Microsoft.Xrm.Sdk.OptionSetValue", "Microsoft.Xrm.Sdk.Money", "Microsoft.Xrm.Sdk.AliasedValue" }.Contains(ptemp))
                {
                    return getAttributeValue(((Microsoft.Xrm.Sdk.AliasedValue)p).Value);
                }

                return ((Microsoft.Xrm.Sdk.AliasedValue)p).Value.ToString();
            }
            else
            {
                return p.ToString();
            }
        }

    }
}
