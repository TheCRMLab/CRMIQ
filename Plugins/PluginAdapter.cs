using System;
using System.Collections.Generic;
using System.Linq;

using System.Reflection;
using System.Text;
using System.Xml;
using System.Xml.Serialization;
using Microsoft.Xrm.Sdk;

using System.IO;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Crm.Sdk.Messages;
using System.Runtime.Serialization.Json;

namespace Cobalt.Components.CrmIQ.Plugin
{
    public static class ParameterName
    {
        public const string EmailId = "EmailId";
        public const string EntityMoniker = "EntityMoniker";
        public const string Id = "Id";
        public const string SubordinateId = "SubordinateId";
        public const string Target = "Target";
        public const string RelatedEntities = "RelatedEntities";
    }
    public enum RequestStage
    {
        PreValidation = 10,
        PreOperation = 20,
        MainOperation = 30,
        PostOperation = 40,
        PostOperationDeprecated = 50
    }
    public class PluginAdapter : IPlugin
    {
        public PluginAdapter(string unsecure, string secure)
        {
        }

        #region IPlugin Members

        public virtual void Execute(IServiceProvider serviceProvider)
        {
            ITracingService tracer = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
            IOrganizationServiceFactory serviceFactory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            try
            {
                IOrganizationService service = serviceFactory.CreateOrganizationService(null);
                IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
                tracer.Trace("Current Depth: {0}", context.Depth);
                Entity preImageEntity = null;
                Entity postImageEntity = null;
                Guid userId = context.InitiatingUserId;
                object[] results = null;
                string messagePropertyName = PluginAdapter.GetMessagePropertyName(context.MessageName);

                if (context.Mode == 0 && context.MessageName.Equals("RetrieveMultiple"))
                {
                    if (context.InputParameters.Contains("Query"))
                    {
                        if (context.InputParameters["Query"] is QueryExpression)
                        {
                            QueryExpression objQueryExpression = (QueryExpression)context.InputParameters["Query"];
                            this.UpdateQuery(service, tracer, objQueryExpression);
                        }
                        else if (context.InputParameters["Query"] is FetchExpression)
                        {
                            FetchExpression objFetchExpression = (FetchExpression)context.InputParameters["Query"];
                            if (this.UpdateQuery(service, tracer, objFetchExpression))
                            {
                                var conversionRequest = new FetchXmlToQueryExpressionRequest
                                {
                                    FetchXml = objFetchExpression.Query
                                };


                                FetchXmlToQueryExpressionResponse fetched = (FetchXmlToQueryExpressionResponse)service.Execute(conversionRequest);
                                fetched.Query.NoLock = true;
                                QueryExpression oneOffQuery = fetched.Query;
                                context.InputParameters["Query"] = UpdateQuery(service, tracer, oneOffQuery);
                            }
                        }
                    }
                }
                else if (context.MessageName.ToLower() == "publish" || context.MessageName.ToLower() == "publishall")
                {
                    CrmMetadata = null;
                }
                else if (context.MessageName.ToLower() == "associate" || context.MessageName.ToLower() == "disassociate")
                {
                    preImageEntity = this.RetrieveTargetEntity(context, serviceFactory, messagePropertyName);
                    postImageEntity = this.RetrieveTargetEntity(context, serviceFactory, ParameterName.RelatedEntities);
                }
                else if (context.Stage == (int)RequestStage.PostOperation || context.Stage == (int)RequestStage.PostOperationDeprecated)
                {
                    preImageEntity = this.RetrievePreImageEntity(context);
                    postImageEntity = this.RetrievePostImageEntity(context);

                    if (postImageEntity == null && context.MessageName != "Delete")
                    {
                        postImageEntity = this.RetrieveTargetEntity(context, serviceFactory, messagePropertyName);
                    }

                    if (postImageEntity == null)
                    {
                        postImageEntity = preImageEntity;
                    }

                    if (preImageEntity == null)
                    {
                        //PlatformContext.Current.EventDispatcher.DispatchEvent(senderInfo, EventSource.Server, context.MessageName, (RequestStage)context.Stage, (context.Mode == (int)Cobalt.Adapters.Platform.Crm2011.PluginRegistration.CrmPluginStepMode.Asynchronous), postImageEntity, postImageEntity, new object[] { context.MessageName, (RequestStage)context.Stage, context.BusinessUnitId, context.InitiatingUserId });
                    }
                    else
                    {
                        //PlatformContext.Current.EventDispatcher.DispatchEvent(senderInfo, EventSource.Server, context.MessageName, (RequestStage)context.Stage, (context.Mode == (int)Cobalt.Adapters.Platform.Crm2011.PluginRegistration.CrmPluginStepMode.Asynchronous), preImageEntity, postImageEntity, new object[] { context.MessageName, (RequestStage)context.Stage, context.BusinessUnitId, context.InitiatingUserId });
                    }

                }
                else //PreOperation, PreValidation, MainOperation
                {
                    preImageEntity = this.RetrievePreImageEntity(context);

                    if (preImageEntity == null)
                    {
                        preImageEntity = this.RetrieveTargetEntity(context, serviceFactory, messagePropertyName);
                    }
                    else
                    {
                        postImageEntity = this.RetrieveTargetEntity(context, serviceFactory, messagePropertyName);
                    }

                    if (postImageEntity != null)
                    {
                        //results = PlatformContext.Current.EventDispatcher.DispatchEvent(senderInfo, EventSource.Server, context.MessageName, (RequestStage)context.Stage, (context.Mode == (int)Cobalt.Adapters.Platform.Crm2011.PluginRegistration.CrmPluginStepMode.Asynchronous), preImageEntity, postImageEntity, new object[] { context.MessageName, (RequestStage)context.Stage, context.BusinessUnitId, context.InitiatingUserId });
                    }
                    else
                    {
                        if (preImageEntity.LogicalName.ToLower() == "cobalt_iqeventhandler")
                        {
                            results = this.HandleIQEvent(service, preImageEntity);
                        }


                        //results = PlatformContext.Current.EventDispatcher.DispatchEvent(senderInfo, EventSource.Server, context.MessageName, (RequestStage)context.Stage, (context.Mode == (int)Cobalt.Adapters.Platform.Crm2011.PluginRegistration.CrmPluginStepMode.Asynchronous), preImageEntity, new object[] { context.MessageName, (RequestStage)context.Stage, context.BusinessUnitId, context.InitiatingUserId });
                    }
                    if (results != null && results.Length > 0)
                    {
                        if (context.InputParameters.Contains(messagePropertyName) && context.InputParameters[messagePropertyName] is Entity)
                        {
                            context.InputParameters[messagePropertyName] = (Entity)results[0];
                        }
                        else if (context.PreEntityImages != null && context.PreEntityImages.Contains("PreImageEntity"))
                        {
                            context.PreEntityImages["PreImageEntity"] = (Entity)results[0];
                        }
                    }
                }
            }
            catch (System.Reflection.ReflectionTypeLoadException rex)
            {
                foreach (Exception loaderException in rex.LoaderExceptions)
                {
                    tracer.Trace(string.Format("Exception caught: {0}", loaderException.ToString()));
                }

                if (rex.InnerException != null)
                {
                    tracer.Trace("Inner Exception: " + rex.InnerException);
                }

                throw new InvalidPluginExecutionException(rex.ToString());
            }
            catch (Exception ex)
            {
                Exception innerException = ex.InnerException;
                while (innerException != null)
                {
                    tracer.Trace(innerException.ToString());
                    System.ServiceModel.FaultException<Microsoft.Xrm.Sdk.OrganizationServiceFault> fe = innerException as System.ServiceModel.FaultException<Microsoft.Xrm.Sdk.OrganizationServiceFault>;
                    if (fe != null && fe.Detail != null)
                    {
                        tracer.Trace("Timestamp: {0}", fe.Detail.Timestamp);
                        tracer.Trace("Code: {0}", fe.Detail.ErrorCode);
                        tracer.Trace("Message: {0}", fe.Detail.ToString());
                        tracer.Trace("Plugin Trace: {0}", fe.Detail.TraceText);
                        tracer.Trace("Inner Fault: {0}", null == fe.Detail.InnerFault ? "Has Inner Fault" : "No Inner Fault");
                    }
                    innerException = innerException.InnerException;
                }
                throw new InvalidPluginExecutionException(ex.ToString());
            }
            finally
            {

            }
        }
        #endregion

        public static SortedDictionary<string, EntityMetadata> CrmMetadata { get; set; }

        private void LoadMetadata(IOrganizationService service)
        {
            if (CrmMetadata == null)
            {
                CrmMetadata = new SortedDictionary<string, EntityMetadata>();

                RetrieveAllEntitiesRequest request = new RetrieveAllEntitiesRequest();
                request.EntityFilters = Microsoft.Xrm.Sdk.Metadata.EntityFilters.Entity;
                RetrieveAllEntitiesResponse response;
                response = (RetrieveAllEntitiesResponse)service.Execute(request);
                if (response != null)
                {
                    foreach (Microsoft.Xrm.Sdk.Metadata.EntityMetadata entity in response.EntityMetadata)
                    {
                 	   lock (CrmMetadata)
                    	{
                        	if (!CrmMetadata.ContainsKey(entity.LogicalName))
	                        {
    	                        CrmMetadata.Add(entity.LogicalName, entity);
        	                }
            	        }
                    }
                }
            }
        }
        private void UpdateQueryExpressionWithCrmIQ(ITracingService tracer, QueryExpression expression)
        {
            if (expression != null && expression.LinkEntities != null)
            {
                int x = 0;
                while (x < expression.LinkEntities.Count())
                {
                    List<FilterExpression> filterExpressionList = null;
                    expression.LinkEntities[x] = CrmIQLinkEntityUpdate(expression, null, expression.LinkEntities[x], out filterExpressionList);
                    if (filterExpressionList != null)
                    {
                        foreach (FilterExpression expr in filterExpressionList)
                        {
                            if (expression.Criteria == null)
                            {
                                expression.Criteria = new FilterExpression();
                            }
                            expression.Criteria.AddFilter(expr);
                        }
                    }
                    x++;
                }
            }
        }

        private void UpdateFilterExpressionRecursive(LinkEntity parentLinkEntity, LinkEntity linkEntity, DataCollection<FilterExpression> filterContainer, List<FilterExpression> outFilterExpressionList, FilterExpression childFilter)
        {
            if (UpdateFilterExpression(parentLinkEntity, linkEntity, childFilter))
            {
                if (outFilterExpressionList == null)
                {
                    outFilterExpressionList = new List<FilterExpression>();
                }

                //Or criteria move all to parent queryexpression
                if (childFilter.FilterOperator == LogicalOperator.Or)
                {
                    outFilterExpressionList.Add(childFilter);
                    if (filterContainer != null)
                    {
                        filterContainer.Remove(childFilter);
                    }
                }
                //And criteria move only the null statement
                else if (childFilter.FilterOperator == LogicalOperator.And)
                {
                    if(childFilter.Conditions != null)
                    {
                        FilterExpression newFilter = new FilterExpression();
                        List<ConditionExpression> newConditions = new List<ConditionExpression>();
                        List<ConditionExpression> newChildFilterCriteriaConditions = new List<ConditionExpression>();
                        for(int i = childFilter.Conditions.Count - 1; i >= 0; i--)
                        {
                            if (CrmMetadata != null && !string.IsNullOrEmpty(linkEntity.LinkToEntityName) && CrmMetadata.ContainsKey(linkEntity.LinkToEntityName) && CrmMetadata[linkEntity.LinkToEntityName].PrimaryIdAttribute == childFilter.Conditions[i].AttributeName && childFilter.Conditions[i].Operator == ConditionOperator.Null)
                            {
                                newConditions.Add(linkEntity.LinkCriteria.Conditions[i]);
                                continue;
                            }
                            childFilter.Conditions[i].EntityName = string.Empty;
                            newChildFilterCriteriaConditions.Add(childFilter.Conditions[i]);
                        }

                        childFilter = new FilterExpression();
                        childFilter.Conditions.AddRange(newChildFilterCriteriaConditions.ToArray());
                        newFilter.Conditions.AddRange(newConditions);
                        outFilterExpressionList.Add(newFilter);
                    }
                }
            }

            if (childFilter != null && childFilter.Filters != null)
            {
                for (int i = childFilter.Filters.Count - 1; i >= 0; i--)
                {
                    FilterExpression subChildFilter = childFilter.Filters[i];
                    UpdateFilterExpressionRecursive(parentLinkEntity, linkEntity, childFilter.Filters, outFilterExpressionList, subChildFilter);
                }
            }
        }

        private bool UpdateFilterExpression(LinkEntity parentLinkEntity, LinkEntity linkEntity, FilterExpression filter)
        {
            int x = 0;
            if (filter != null && filter.Conditions != null && linkEntity != null)
            {
                while (x < filter.Conditions.Count())
                {
                    if (CrmMetadata != null && !string.IsNullOrEmpty(linkEntity.LinkToEntityName) && CrmMetadata.ContainsKey(linkEntity.LinkToEntityName) && CrmMetadata[linkEntity.LinkToEntityName].PrimaryIdAttribute == filter.Conditions[x].AttributeName && filter.Conditions[x].Operator == ConditionOperator.Null)
                    {
                        if (parentLinkEntity != null && !string.IsNullOrEmpty(parentLinkEntity.LinkToEntityName))
                        {
                            if (CrmMetadata.ContainsKey(parentLinkEntity.LinkToEntityName) && CrmMetadata[parentLinkEntity.LinkToEntityName].IsIntersect != null && CrmMetadata[parentLinkEntity.LinkToEntityName].IsIntersect.Value)
                            {
                                if (string.IsNullOrEmpty(parentLinkEntity.EntityAlias))
                                {
                                    parentLinkEntity.EntityAlias = string.Format("a{0}", Guid.NewGuid().ToString().Replace("-", ""));
                                }
                                parentLinkEntity.JoinOperator = JoinOperator.LeftOuter;
                            }
                        }
                        linkEntity.JoinOperator = JoinOperator.LeftOuter;
                        foreach (ConditionExpression condition in filter.Conditions)
                        {
                            if (string.IsNullOrEmpty(linkEntity.EntityAlias))
                            {
                                linkEntity.EntityAlias = string.Format("a{0}", Guid.NewGuid().ToString().Replace("-", ""));
                            }
                            condition.EntityName = linkEntity.EntityAlias;
                        }
                        return true;
                    }

                    x++;
                }
            }
            return false;
        }

        private LinkEntity CrmIQLinkEntityUpdate(QueryExpression expression, LinkEntity parentLinkEntity, LinkEntity linkEntity, out List<FilterExpression> outFilterExpressionList)
        {
            outFilterExpressionList = new List<FilterExpression>();
            if (expression != null)
            {
                if (linkEntity != null && linkEntity.LinkCriteria != null && linkEntity.LinkCriteria.Conditions != null)
                {
                    if (UpdateFilterExpression(parentLinkEntity, linkEntity, linkEntity.LinkCriteria))
                    {
                        //Or criteria move all to parent queryexpression
                        if (linkEntity.LinkCriteria.FilterOperator == LogicalOperator.Or)
                        {
                            outFilterExpressionList.Add(linkEntity.LinkCriteria);
                            linkEntity.LinkCriteria = new FilterExpression();
                        }
                        //And criteria move only the null statement
                        else if (linkEntity.LinkCriteria.FilterOperator == LogicalOperator.And)
                        {
                            FilterExpression newFilter = new FilterExpression();
                            List<ConditionExpression> newConditions = new List<ConditionExpression>();
                            List<ConditionExpression> newLinkCriteriaConditions = new List<ConditionExpression>();
                            for (int i = linkEntity.LinkCriteria.Conditions.Count - 1; i >= 0; i--)
                            {
                                if (CrmMetadata != null && !string.IsNullOrEmpty(linkEntity.LinkToEntityName) && CrmMetadata.ContainsKey(linkEntity.LinkToEntityName) && CrmMetadata[linkEntity.LinkToEntityName].PrimaryIdAttribute == linkEntity.LinkCriteria.Conditions[i].AttributeName && linkEntity.LinkCriteria.Conditions[i].Operator == ConditionOperator.Null)
                                {
                                    newConditions.Add(linkEntity.LinkCriteria.Conditions[i]);
                                    continue;
                                }
                                linkEntity.LinkCriteria.Conditions[i].EntityName = string.Empty;
                                newLinkCriteriaConditions.Add(linkEntity.LinkCriteria.Conditions[i]);
                            }
                            linkEntity.LinkCriteria = new FilterExpression();
                            linkEntity.LinkCriteria.Conditions.AddRange(newLinkCriteriaConditions.ToArray());
                            newFilter.Conditions.AddRange(newConditions);
                            outFilterExpressionList.Add(newFilter);
                        }
                    }
                    if (linkEntity.LinkCriteria != null && linkEntity.LinkCriteria.Filters != null)
                    {
                        for (int i = linkEntity.LinkCriteria.Filters.Count - 1; i >= 0; i--)
                        {
                            FilterExpression childFilter = linkEntity.LinkCriteria.Filters[i];
                            UpdateFilterExpressionRecursive(parentLinkEntity, linkEntity, linkEntity.LinkCriteria.Filters, outFilterExpressionList, childFilter);
                        }
                    }
                }

                if (linkEntity != null && linkEntity.LinkEntities != null)
                {
                    int x = 0;
                    while (x < linkEntity.LinkEntities.Count())
                    {
                        List<FilterExpression> filterExpressionList = null;
                        linkEntity.LinkEntities[x] = CrmIQLinkEntityUpdate(expression, linkEntity, linkEntity.LinkEntities[x], out filterExpressionList);

                        foreach (FilterExpression expr in filterExpressionList)
                        {
                            if (expression.Criteria == null)
                            {
                                expression.Criteria = new FilterExpression();
                            }
                            expression.Criteria.AddFilter(expr);
                        }
                        x++;
                    }
                }
            }
            return linkEntity;
        }

        private bool UpdateFetchExpressionWithCrmIQ(ITracingService tracer, FetchExpression expression)
        {
            if (expression != null && !string.IsNullOrEmpty(expression.Query))
            {
                bool fetchUpdated = false;
                FetchXmlExpression xmlExpression = FetchXmlExpression.Deserialize(expression.Query);

                if (xmlExpression != null && xmlExpression.Items != null)
                {
                    int x = 0;
                    while (x < xmlExpression.Items.Count())
                    {
                        object item = xmlExpression.Items[x];
                        if (item is FetchEntity)
                        {
                            if (((FetchEntity)item).Items != null)
                            {
                                for (int i = ((FetchEntity)item).Items.Length - 1; i >= 0; i--)
                                {
                                    object entityItem = ((FetchEntity)item).Items[i];
                                    if (entityItem != null && entityItem is FetchLinkEntity)
                                    {
                                        List<FetchFilter> filterExpressionList = null;
                                        if (CrmIQLinkEntityUpdate(xmlExpression, null, (FetchLinkEntity)entityItem, out filterExpressionList))
                                        {
                                            fetchUpdated = true;
                                            if (filterExpressionList != null)
                                            {
                                                foreach (FetchFilter expr in filterExpressionList)
                                                {
                                                    FetchFilter expressionCriteria = this.RetrieveExpressionCriteria(xmlExpression);
                                                    if (expressionCriteria == null)
                                                    {
                                                        expressionCriteria = new FetchFilter();
                                                        List<object> list = new List<object>(((FetchEntity)item).Items);
                                                        list.Add(expressionCriteria);
                                                        ((FetchEntity)item).Items = list.ToArray();
                                                    }
                                                    List<object> filters = new List<object>();
                                                    if (expressionCriteria.Items != null)
                                                    {
                                                        filters.AddRange(expressionCriteria.Items);
                                                    }
                                                    filters.Add(expr);
                                                    expressionCriteria.Items = filters.ToArray();
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        x++;
                    }
                }
                return fetchUpdated;
            }
            return false;
        }

        private bool CrmIQLinkEntityUpdate(FetchXmlExpression expression, FetchLinkEntity parentLinkEntity, FetchLinkEntity linkEntity, out List<FetchFilter> outFilterExpressionList)
        {
            bool returnValue = false;
            outFilterExpressionList = new List<FetchFilter>();
            if (linkEntity != null)
            {
                FetchFilter linkCriteria = this.RetrieveLinkCriteria(linkEntity);
                if (linkCriteria != null)
                {
                    if (UpdateFilterExpression(parentLinkEntity, linkEntity, linkCriteria))
                    {
                        returnValue = true;
                        //Or criteria move all to parent fetchexpression
                        if (linkCriteria.type == FetchFilterType.or)
                        {
                            outFilterExpressionList.Add(linkCriteria);
                            if (linkEntity.Items != null)
                            {
                                for (int i = 0; i < linkEntity.Items.Length; i++)
                                {
                                    if (linkEntity.Items[i] is FetchFilter)
                                    {
                                        linkEntity.Items[i] = new FetchFilter();
                                        break;
                                    }
                                }
                            }
                        }
                        //And criteria move only the null statement
                        else if (linkCriteria.type == FetchFilterType.and)
                        {
                            FetchFilter newFilter = new FetchFilter();
                            List<FetchCondition> newConditions = new List<FetchCondition>();
                            if (linkEntity.Items != null)
                            {
                                for (int i = 0; i < linkEntity.Items.Length; i++)
                                {
                                    FetchFilter innerFilter = linkEntity.Items[i] as FetchFilter;
                                    if (innerFilter != null)
                                    {
                                        if(innerFilter.Items != null)
                                        {
                                            List<object> newInnerFilterItems = new List<object>();
                                            for (int j = innerFilter.Items.Length - 1; j >= 0; j--)
                                            {
                                                FetchCondition innerCondition = innerFilter.Items[j] as FetchCondition;
                                                if(innerCondition != null)
                                                {
                                                    if (CrmMetadata != null && linkEntity != null && CrmMetadata.ContainsKey(linkEntity.name) && CrmMetadata[linkEntity.name].PrimaryIdAttribute == innerCondition.attribute && innerCondition.@operator == FetchOperator.@null)
                                                    {
                                                        newConditions.Add(innerCondition);
                                                        continue;
                                                    }
                                                }
                                                newInnerFilterItems.Add(innerFilter.Items[j]);
                                            }
                                            innerFilter.Items = newInnerFilterItems.ToArray();
                                        }
                                    }
                                }
                            }
                            newFilter.Items = newConditions.ToArray();
                            outFilterExpressionList.Add(newFilter);
                        }
                    }
                    if (linkCriteria != null && linkCriteria.Items != null)
                    {
                        for (int i = linkCriteria.Items.Length - 1; i >= 0; i--)
                        {
                            if (i > linkCriteria.Items.Length - 1)
                            {
                                i--;
                            }
                            FetchFilter childFilter = linkCriteria.Items[i] as FetchFilter;
                            if (childFilter != null)
                            {
                                if (UpdateFetchFilterExpressionRecursive(parentLinkEntity, linkEntity, linkCriteria.Items, outFilterExpressionList, childFilter))
                                {
                                    returnValue = true;
                                    List<object> filterList = new List<object>(linkCriteria.Items);
                                    filterList.Remove(childFilter);
                                    linkCriteria.Items = filterList.ToArray();
                                }
                            }
                        }
                    }
                }
            }

            if (linkEntity != null)
            {
                List<FetchLinkEntity> childLinks = this.RetrieveLinkEntities(linkEntity);
                if (childLinks != null)
                {
                    int x = 0;
                    while (x < childLinks.Count)
                    {
                        List<FetchFilter> filterExpressionList = null;
                        if (CrmIQLinkEntityUpdate(expression, linkEntity, childLinks[x], out filterExpressionList))
                        {
                            returnValue = true;
                            FetchFilter expressionCriteria = RetrieveExpressionCriteria(expression);
                            if (expressionCriteria == null)
                            {
                                expressionCriteria = new FetchFilter();
                                List<object> list = new List<object>(((FetchEntity)expression.Items[0]).Items);
                                list.Add(expressionCriteria);
                                ((FetchEntity)expression.Items[0]).Items = list.ToArray();
                            }
                            if (filterExpressionList != null)
                            {
                                foreach (FetchFilter expr in filterExpressionList)
                                {
                                    List<object> filters = new List<object>();
                                    if (expressionCriteria.Items != null)
                                    {
                                        filters.AddRange(expressionCriteria.Items);
                                    }
                                    filters.Add(expr);
                                    expressionCriteria.Items = filters.ToArray();
                                }
                            }
                        }
                        x++;
                    }
                }
            }
            return returnValue;
        }

        private bool UpdateFetchFilterExpressionRecursive(FetchLinkEntity parentLinkEntity, FetchLinkEntity linkEntity, object[] filterContainer, List<FetchFilter> outFilterExpressionList, FetchFilter childFilter)
        {
            bool returnValue = false;
            if (UpdateFilterExpression(parentLinkEntity, linkEntity, childFilter))
            {
                if (outFilterExpressionList == null)
                {
                    outFilterExpressionList = new List<FetchFilter>();
                }
                outFilterExpressionList.Add(childFilter);
                returnValue = true;
            }

            if (childFilter.Items != null)
            {
                for (int i = childFilter.Items.Length - 1; i >= 0; i--)
                {
                    FetchFilter subChildFilter = childFilter.Items[i] as FetchFilter;
                    if (subChildFilter != null)
                    {
                        if (UpdateFetchFilterExpressionRecursive(parentLinkEntity, linkEntity, childFilter.Items, outFilterExpressionList, subChildFilter))
                        {
                            List<object> filterList = new List<object>(childFilter.Items);
                            filterList.Remove(subChildFilter);
                            childFilter.Items = filterList.ToArray();
                        }
                    }
                }
            }
            return returnValue;
        }

        private bool UpdateFilterExpression(FetchLinkEntity parentLinkEntity, FetchLinkEntity linkEntity, FetchFilter filter)
        {
            int x = 0;
            List<FetchCondition> filterConditions = this.RetrieveFilterCondtions(filter);
            if (filterConditions != null)
            {
                while (x < filterConditions.Count)
                {
                    if (CrmMetadata != null && linkEntity != null && CrmMetadata.ContainsKey(linkEntity.name) && CrmMetadata[linkEntity.name].PrimaryIdAttribute == filterConditions[x].attribute && filterConditions[x].@operator == FetchOperator.@null)
                    {
                        if (parentLinkEntity != null && !string.IsNullOrEmpty(parentLinkEntity.name))
                        {
                            if (CrmMetadata.ContainsKey(parentLinkEntity.name) && CrmMetadata[parentLinkEntity.name].IsIntersect != null && CrmMetadata[parentLinkEntity.name].IsIntersect.Value)
                            {
                                if (string.IsNullOrEmpty(parentLinkEntity.alias))
                                {
                                    parentLinkEntity.alias = string.Format("a{0}", Guid.NewGuid().ToString().Replace("-", ""));
                                }
                                parentLinkEntity.linktype = LinkType.outer.ToString();
                            }
                        }
                        linkEntity.linktype = LinkType.outer.ToString();
                        foreach (FetchCondition condition in filterConditions)
                        {
                            if (string.IsNullOrEmpty(linkEntity.alias))
                            {
                                linkEntity.alias = string.Format("a{0}", Guid.NewGuid().ToString().Replace("-", ""));
                            }
                            condition.entityname = linkEntity.alias;
                        }
                        return true;
                    }

                    x++;
                }
            }
            return false;
        }

        private List<FetchCondition> RetrieveFilterCondtions(FetchFilter filter)
        {
            if (filter != null && filter.Items != null)
            {
                return filter.Items.OfType<FetchCondition>().ToList();
            }
            return null;
        }
        private List<FetchLinkEntity> RetrieveLinkEntities(FetchLinkEntity link)
        {
            if (link != null && link.Items != null)
            {
                return link.Items.OfType<FetchLinkEntity>().ToList();
            }
            return null;
        }

        private FetchFilter RetrieveExpressionCriteria(FetchXmlExpression expression)
        {
            if (expression != null && expression.Items != null && expression.Items.Length > 0 && expression.Items[0] is FetchEntity)
            {
                return ((FetchEntity)expression.Items[0]).Items.OfType<FetchFilter>().FirstOrDefault();
            }
            return null;
        }


        private FetchFilter RetrieveLinkCriteria(FetchLinkEntity link)
        {
            if (link != null && link.Items != null)
            {
                return link.Items.OfType<FetchFilter>().FirstOrDefault();
            }
            return null;
        }

        public QueryExpression UpdateQuery(IOrganizationService service, ITracingService tracer, QueryExpression query)
        {
            this.LoadMetadata(service);
            UpdateQueryExpressionWithCrmIQ(tracer, query);
            return query;
        }

        public bool UpdateQuery(IOrganizationService service, ITracingService tracer, FetchExpression query)
        {
            this.LoadMetadata(service);
            return UpdateFetchExpressionWithCrmIQ(tracer, query);
        }

        public string Serialize(QueryExpression query)
        {
            XmlSerializer serializer = new XmlSerializer(typeof(QueryExpression));
            StringWriter writer = new StringWriter();

            serializer.Serialize(writer, query);
            string xmlString = writer.ToString();

            System.Xml.XmlDocument document = new System.Xml.XmlDocument();
            document.LoadXml(xmlString);
            xmlString = document.OuterXml;
            return xmlString;
        }

        private Entity RetrievePreImageEntity(IPluginExecutionContext context)
        {
            Entity preImageEntity = null;
            if (context.PreEntityImages != null && context.PreEntityImages.Contains("PreImageEntity"))
            {
                preImageEntity = context.PreEntityImages["PreImageEntity"];
            }
            return preImageEntity;
        }

        private Entity RetrievePostImageEntity(IPluginExecutionContext context)
        {
            Entity postImageEntity = null;
            if (context.PostEntityImages != null && context.PostEntityImages.Contains("PostImageEntity"))
            {
                postImageEntity = context.PostEntityImages["PostImageEntity"];
            }
            return postImageEntity;
        }
        private Entity RetrieveTargetEntity(IPluginExecutionContext context, IOrganizationServiceFactory serviceFactory, string messagePropertyName)
        {
            Entity returnValue = null;
            if (context.InputParameters.Contains(messagePropertyName))
            {
                IOrganizationService service = serviceFactory.CreateOrganizationService(null);
                if (context.InputParameters[messagePropertyName] is Entity)
                {
                    returnValue = (Entity)context.InputParameters[messagePropertyName];
                }
                else if (context.InputParameters[messagePropertyName] is EntityReference || context.InputParameters[messagePropertyName] is EntityReferenceCollection)
                {
                    EntityReference moniker = null;

                    EntityReferenceCollection collection = context.InputParameters[messagePropertyName] as EntityReferenceCollection;
                    if (collection != null)
                    {
                        if (collection.Count > 0)
                        {
                            moniker = collection[0];
                        }
                    }
                    else
                    {
                        moniker = (EntityReference)context.InputParameters[messagePropertyName];
                    }
                    returnValue = service.Retrieve(moniker.LogicalName, moniker.Id, new Microsoft.Xrm.Sdk.Query.ColumnSet(true));
                    if (context.InputParameters.Contains("State"))
                    {
                        if (context.InputParameters["State"] is OptionSetValue)
                        {
                            //MODEBUG returnValue["statecode"] = returnValue.GetStringStateCode(((OptionSetValue)context.InputParameters["State"]).Value);
                        }
                        else if (context.InputParameters["State"] is string)
                        {
                            returnValue["statecode"] = context.InputParameters["State"].ToString();
                        }
                    }
                    if (context.InputParameters.Contains("Status"))
                    {
                        if (context.InputParameters["Status"] is OptionSetValue)
                        {
                            //MODEBUG returnValue.SetPropertyValue("statuscode", ((OptionSetValue)context.InputParameters["Status"]).Value);
                        }
                        else if (context.InputParameters["Status"] is int)
                        {
                            returnValue["statuscode"] = (int)context.InputParameters["Status"];
                        }
                    }
                }
                else if (messagePropertyName == ParameterName.EmailId)
                {
                    Guid emailId = (Guid)context.InputParameters[messagePropertyName];
                    returnValue = service.Retrieve("email", emailId, new Microsoft.Xrm.Sdk.Query.ColumnSet(true));
                }
            }
            else if (context.OutputParameters.Contains(messagePropertyName))
            {
                if (context.InputParameters[messagePropertyName] is Entity)
                {
                    returnValue = (Entity)context.InputParameters[messagePropertyName];
                }
            }
            else if (context.PrimaryEntityId != null && context.PrimaryEntityId != Guid.Empty && !string.IsNullOrEmpty(context.PrimaryEntityName))
            {
                returnValue = new Entity(context.PrimaryEntityName);
                returnValue.Id = context.PrimaryEntityId;
            }
            return returnValue;
        }

        #region Protected Methods
        protected static string GetMessagePropertyName(string messageName)
        {
            switch (messageName)
            {
                case "DeliverIncoming":
                    return ParameterName.EmailId;
                case "DeliverPromote":
                    return ParameterName.EmailId;
                case "Retrieve":
                    return "BusinessEntity";
                case "Send":
                    return ParameterName.EmailId;
                case "SetState":
                    return ParameterName.EntityMoniker;
                case "SetStateEntity":
                    return ParameterName.EntityMoniker;
                default:
                    return ParameterName.Target;
            }
        }

        
        public Entity[] HandleIQEvent(IOrganizationService service, Entity imageEntity)
        {
            this.LoadMetadata(service);

            if (imageEntity.Attributes.ContainsKey("cobalt_name"))
            {
                string instructionName = imageEntity.Attributes["cobalt_name"].ToString();
                string request = imageEntity.Attributes.ContainsKey("cobalt_request") ? imageEntity.Attributes["cobalt_request"].ToString() : String.Empty;

                Assembly assembly = Assembly.GetExecutingAssembly();
                Type type = assembly.GetTypes()
                    .FirstOrDefault(t =>
                        t.IsSubclassOf(typeof(Cobalt.Components.CrmIQ.Plugin.Instructions.Instruction)) &&
                        t.GetCustomAttribute<Cobalt.Components.CrmIQ.Plugin.Instructions.InstructionName>().Instruction == instructionName);
                if (type == null)
                {
                    throw new ArgumentException(String.Format("Come on man, I can't find an instruction with name '{0}'", instructionName));
                }
                Cobalt.Components.CrmIQ.Plugin.Instructions.Instruction instruction = null;
                if (String.IsNullOrEmpty(request))
                {
                    instruction = (Cobalt.Components.CrmIQ.Plugin.Instructions.Instruction)Activator.CreateInstance(type);
                }
                else using (MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes(request)))
                {
                    instruction = new DataContractJsonSerializer(type).ReadObject(stream) as Cobalt.Components.CrmIQ.Plugin.Instructions.Instruction;
                }

                if (instruction != null)
                {
                    instruction.Service = service;
                    imageEntity.Attributes["cobalt_response"] = instruction.Execute();
                }
            }

            return new Entity[] { imageEntity };
        }

        #endregion
    }
}
