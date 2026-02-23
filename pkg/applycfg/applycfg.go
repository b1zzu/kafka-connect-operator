/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package applycfg provides converters from standard Kubernetes API types
// to their corresponding client-go apply configuration types.
package applycfg

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
)

// ResourceRequirements converts a corev1.ResourceRequirements to its apply configuration.
func ResourceRequirements(r *corev1.ResourceRequirements) *corev1ac.ResourceRequirementsApplyConfiguration {
	if r == nil {
		return nil
	}
	ac := &corev1ac.ResourceRequirementsApplyConfiguration{}
	if r.Requests != nil {
		ac.Requests = &r.Requests
	}
	if r.Limits != nil {
		ac.Limits = &r.Limits
	}
	return ac
}

// TopologySpreadConstraints converts a slice of corev1.TopologySpreadConstraint
// to a slice of apply configuration pointers.
func TopologySpreadConstraints(
	constraints []corev1.TopologySpreadConstraint,
) []*corev1ac.TopologySpreadConstraintApplyConfiguration {
	acs := make([]*corev1ac.TopologySpreadConstraintApplyConfiguration, len(constraints))
	for i := range constraints {
		acs[i] = topologySpreadConstraint(&constraints[i])
	}
	return acs
}

func topologySpreadConstraint(c *corev1.TopologySpreadConstraint) *corev1ac.TopologySpreadConstraintApplyConfiguration {
	ac := &corev1ac.TopologySpreadConstraintApplyConfiguration{
		MaxSkew:            &c.MaxSkew,
		TopologyKey:        &c.TopologyKey,
		WhenUnsatisfiable:  &c.WhenUnsatisfiable,
		LabelSelector:      LabelSelector(c.LabelSelector),
		MinDomains:         c.MinDomains,
		NodeAffinityPolicy: c.NodeAffinityPolicy,
		NodeTaintsPolicy:   c.NodeTaintsPolicy,
	}
	if len(c.MatchLabelKeys) > 0 {
		ac.MatchLabelKeys = c.MatchLabelKeys
	}
	return ac
}

// Tolerations converts a slice of corev1.Toleration to a slice of apply configuration pointers.
func Tolerations(tolerations []corev1.Toleration) []*corev1ac.TolerationApplyConfiguration {
	acs := make([]*corev1ac.TolerationApplyConfiguration, len(tolerations))
	for i := range tolerations {
		acs[i] = toleration(&tolerations[i])
	}
	return acs
}

func toleration(t *corev1.Toleration) *corev1ac.TolerationApplyConfiguration {
	return &corev1ac.TolerationApplyConfiguration{
		Key:               &t.Key,
		Operator:          &t.Operator,
		Value:             &t.Value,
		Effect:            &t.Effect,
		TolerationSeconds: t.TolerationSeconds,
	}
}

// Affinity converts a corev1.Affinity to its apply configuration.
func Affinity(a *corev1.Affinity) *corev1ac.AffinityApplyConfiguration {
	if a == nil {
		return nil
	}
	return &corev1ac.AffinityApplyConfiguration{
		NodeAffinity:    nodeAffinity(a.NodeAffinity),
		PodAffinity:     podAffinity(a.PodAffinity),
		PodAntiAffinity: podAntiAffinity(a.PodAntiAffinity),
	}
}

func nodeAffinity(na *corev1.NodeAffinity) *corev1ac.NodeAffinityApplyConfiguration {
	if na == nil {
		return nil
	}
	ac := &corev1ac.NodeAffinityApplyConfiguration{}
	if na.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		req := na.RequiredDuringSchedulingIgnoredDuringExecution
		terms := make([]corev1ac.NodeSelectorTermApplyConfiguration, len(req.NodeSelectorTerms))
		for i, term := range req.NodeSelectorTerms {
			terms[i] = nodeSelectorTerm(term)
		}
		ac.RequiredDuringSchedulingIgnoredDuringExecution = &corev1ac.NodeSelectorApplyConfiguration{
			NodeSelectorTerms: terms,
		}
	}
	for _, pref := range na.PreferredDuringSchedulingIgnoredDuringExecution {
		t := nodeSelectorTerm(pref.Preference)
		ac.PreferredDuringSchedulingIgnoredDuringExecution = append(
			ac.PreferredDuringSchedulingIgnoredDuringExecution,
			corev1ac.PreferredSchedulingTermApplyConfiguration{
				Weight:     &pref.Weight,
				Preference: &t,
			},
		)
	}
	return ac
}

func nodeSelectorTerm(term corev1.NodeSelectorTerm) corev1ac.NodeSelectorTermApplyConfiguration {
	ac := corev1ac.NodeSelectorTermApplyConfiguration{}
	for _, expr := range term.MatchExpressions {
		ac.MatchExpressions = append(ac.MatchExpressions, corev1ac.NodeSelectorRequirementApplyConfiguration{
			Key:      &expr.Key,
			Operator: &expr.Operator,
			Values:   expr.Values,
		})
	}
	for _, field := range term.MatchFields {
		ac.MatchFields = append(ac.MatchFields, corev1ac.NodeSelectorRequirementApplyConfiguration{
			Key:      &field.Key,
			Operator: &field.Operator,
			Values:   field.Values,
		})
	}
	return ac
}

func podAffinity(pa *corev1.PodAffinity) *corev1ac.PodAffinityApplyConfiguration {
	if pa == nil {
		return nil
	}
	ac := &corev1ac.PodAffinityApplyConfiguration{}
	for _, term := range pa.RequiredDuringSchedulingIgnoredDuringExecution {
		ac.RequiredDuringSchedulingIgnoredDuringExecution = append(
			ac.RequiredDuringSchedulingIgnoredDuringExecution,
			podAffinityTerm(term),
		)
	}
	for _, pref := range pa.PreferredDuringSchedulingIgnoredDuringExecution {
		t := podAffinityTerm(pref.PodAffinityTerm)
		ac.PreferredDuringSchedulingIgnoredDuringExecution = append(
			ac.PreferredDuringSchedulingIgnoredDuringExecution,
			corev1ac.WeightedPodAffinityTermApplyConfiguration{
				Weight:          &pref.Weight,
				PodAffinityTerm: &t,
			},
		)
	}
	return ac
}

func podAntiAffinity(paa *corev1.PodAntiAffinity) *corev1ac.PodAntiAffinityApplyConfiguration {
	if paa == nil {
		return nil
	}
	ac := &corev1ac.PodAntiAffinityApplyConfiguration{}
	for _, term := range paa.RequiredDuringSchedulingIgnoredDuringExecution {
		ac.RequiredDuringSchedulingIgnoredDuringExecution = append(
			ac.RequiredDuringSchedulingIgnoredDuringExecution,
			podAffinityTerm(term),
		)
	}
	for _, pref := range paa.PreferredDuringSchedulingIgnoredDuringExecution {
		t := podAffinityTerm(pref.PodAffinityTerm)
		ac.PreferredDuringSchedulingIgnoredDuringExecution = append(
			ac.PreferredDuringSchedulingIgnoredDuringExecution,
			corev1ac.WeightedPodAffinityTermApplyConfiguration{
				Weight:          &pref.Weight,
				PodAffinityTerm: &t,
			},
		)
	}
	return ac
}

func podAffinityTerm(term corev1.PodAffinityTerm) corev1ac.PodAffinityTermApplyConfiguration {
	ac := corev1ac.PodAffinityTermApplyConfiguration{
		TopologyKey:   &term.TopologyKey,
		LabelSelector: LabelSelector(term.LabelSelector),
	}
	if len(term.Namespaces) > 0 {
		ac.Namespaces = term.Namespaces
	}
	return ac
}

// LabelSelector converts a metav1.LabelSelector to its apply configuration.
func LabelSelector(ls *metav1.LabelSelector) *metav1ac.LabelSelectorApplyConfiguration {
	if ls == nil {
		return nil
	}
	ac := &metav1ac.LabelSelectorApplyConfiguration{
		MatchLabels: ls.MatchLabels,
	}
	for _, expr := range ls.MatchExpressions {
		ac.MatchExpressions = append(ac.MatchExpressions, metav1ac.LabelSelectorRequirementApplyConfiguration{
			Key:      &expr.Key,
			Operator: &expr.Operator,
			Values:   expr.Values,
		})
	}
	return ac
}
