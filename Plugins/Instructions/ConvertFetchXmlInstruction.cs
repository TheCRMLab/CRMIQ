using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Json;

namespace Cobalt.Components.CrmIQ.Plugin.Instructions
{
    [DataContract]
    [Serializable()]
    [InstructionName("ConvertFetchXml")]
    public class ConvertFetchXmlInstruction : Instruction
    {
        public ConvertFetchXmlInstruction()
        {
            this.FetchXmlToEvaluate = string.Empty;
        }

        public override string Execute()
        {
            this.FetchXmlToEvaluate = this.FetchXmlToEvaluate.Replace("\\", "");
            this.FetchXmlToEvaluate = this.FetchXmlToEvaluate.Replace('\'', '"');
            FetchExpression fetchXmlResponse = new FetchExpression(this.FetchXmlToEvaluate);
            ConvertFetchXmlResponse response = new ConvertFetchXmlResponse();

            var queryConversionRequest = new FetchXmlToQueryExpressionRequest
            {
                FetchXml = fetchXmlResponse.Query
            };

            FetchXmlToQueryExpressionResponse fetched = (FetchXmlToQueryExpressionResponse)this.Service.Execute(queryConversionRequest);
            fetched.Query.NoLock = true;

            var xmlConversionRequest = new QueryExpressionToFetchXmlRequest
            {
                Query = UpdateQuery(this.Service, fetched.Query)
            };

            QueryExpressionToFetchXmlResponse result = (QueryExpressionToFetchXmlResponse)this.Service.Execute(xmlConversionRequest);
            response.FetchXmlResponse = result.FetchXml;

            MemoryStream stream = new MemoryStream();
            DataContractJsonSerializer serializer = new DataContractJsonSerializer(typeof(ConvertFetchXmlResponse));
            serializer.WriteObject(stream, response);
            stream.Position = 0;
            StreamReader streamReader = new StreamReader(stream);
            string returnValue = streamReader.ReadToEnd();

            return returnValue;
        }

        private void UpdateQueryExpressionWithCrmIQ(QueryExpression expression)
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
                outFilterExpressionList.Add(childFilter);
                if (filterContainer != null)
                {
                    filterContainer.Remove(childFilter);
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
                    if (this.MetaDataService != null && !string.IsNullOrEmpty(linkEntity.LinkToEntityName) && this.MetaDataService.RetrieveMetadata(linkEntity.LinkToEntityName) != null && this.MetaDataService.RetrieveMetadata(linkEntity.LinkToEntityName).PrimaryIdAttribute == filter.Conditions[x].AttributeName && filter.Conditions[x].Operator == ConditionOperator.Null)
                    {
                        if (parentLinkEntity != null && !string.IsNullOrEmpty(parentLinkEntity.LinkToEntityName))
                        {
                            if (this.MetaDataService.RetrieveMetadata(parentLinkEntity.LinkToEntityName) != null && this.MetaDataService.IsIntersect(parentLinkEntity.LinkToEntityName))
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
                        outFilterExpressionList.Add(linkEntity.LinkCriteria);
                        linkEntity.LinkCriteria = new FilterExpression();
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

        private bool UpdateFetchExpressionWithCrmIQ(FetchExpression expression)
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
                    if (this.MetaDataService != null && linkEntity != null && this.MetaDataService.RetrieveMetadata(linkEntity.name) != null && this.MetaDataService.RetrieveMetadata(linkEntity.name).PrimaryIdAttribute == filterConditions[x].attribute && filterConditions[x].@operator == FetchOperator.@null)
                    {
                        if (parentLinkEntity != null && !string.IsNullOrEmpty(parentLinkEntity.name))
                        {
                            if (this.MetaDataService.RetrieveMetadata(parentLinkEntity.name) != null && this.MetaDataService.IsIntersect(linkEntity.name))
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

        public QueryExpression UpdateQuery(IOrganizationService service, QueryExpression query)
        {
            UpdateQueryExpressionWithCrmIQ(query);
            return query;
        }

        public bool UpdateQuery(IOrganizationService service, FetchExpression query)
        {
            return UpdateFetchExpressionWithCrmIQ(query);
        }

        [DataMember(EmitDefaultValue = false)]
        public string FetchXmlToEvaluate { get; set; }
    }
}
