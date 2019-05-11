#region copyright
// -----------------------------------------------------------------------
//  <copyright file="EndpointFilterSpec.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2015-2019 Red Bull Media House GmbH <http://www.redbullmediahouse.com>
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System.Collections.Immutable;
using FluentAssertions;
using Xunit;
using static Eventuate.EndpointFilters;

namespace Eventuate.Tests
{
    public class EndpointFilterSpec
    {
        private class NewFilter : ReplicationFilter
        {
            public override bool Invoke(DurableEvent durableEvent) => true;
        }
        
        private readonly ReplicationFilter targetFilter = new NewFilter();
        private readonly ReplicationFilter sourceFilter = new NewFilter();
        private readonly string targetLogId = "targetLogId";
        private readonly string sourceLogName = "sourceLogName";
        private readonly ImmutableDictionary<string, ReplicationFilter> targetFilters;
        private readonly ImmutableDictionary<string, ReplicationFilter> sourceFilters;

        public EndpointFilterSpec()
        {
            targetFilters = ImmutableDictionary<string, ReplicationFilter>.Empty.Add(targetLogId, targetFilter);
            sourceFilters = ImmutableDictionary<string, ReplicationFilter>.Empty.Add(sourceLogName, sourceFilter);
        }

        [Fact]
        public void EndpointFilters_must_and_source_and_target_filters()
        {
            var endpointFilters = TargetAndSourceFilters(targetFilters, sourceFilters);

            endpointFilters.FilterFor(targetLogId, sourceLogName).Should().Be(targetFilter.And(sourceFilter));
            endpointFilters.FilterFor("", sourceLogName).Should().Be(sourceFilter);
            endpointFilters.FilterFor(targetLogId, "").Should().Be(targetFilter);
            endpointFilters.FilterFor("", "").Should().Be(NoFilter.Instance);
        }
        
        [Fact]
        public void EndpointFilters_must_override_source_by_target_filters()
        {
            var endpointFilters = TargetOverridesSourceFilters(targetFilters, sourceFilters);

            endpointFilters.FilterFor(targetLogId, sourceLogName).Should().Be(targetFilter);
            endpointFilters.FilterFor("", sourceLogName).Should().Be(sourceFilter);
            endpointFilters.FilterFor(targetLogId, "").Should().Be(targetFilter);
            endpointFilters.FilterFor("", "").Should().Be(NoFilter.Instance);
        }
        
        [Fact]
        public void EndpointFilters_must_use_source_filters_only()
        {
            var endpointFilters = SourceFilters(sourceFilters);

            endpointFilters.FilterFor(targetLogId, sourceLogName).Should().Be(sourceFilter);
            endpointFilters.FilterFor(targetLogId, "").Should().Be(NoFilter.Instance);   
        }
        
        [Fact]
        public void EndpointFilters_must_use_target_filters_only()
        {
            var endpointFilters = TargetFilters(targetFilters);

            endpointFilters.FilterFor(targetLogId, sourceLogName).Should().Be(targetFilter);
            endpointFilters.FilterFor("", sourceLogName).Should().Be(NoFilter.Instance);  
        }
    }
}