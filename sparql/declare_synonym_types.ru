PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

# Before: linkml-owl emits hasSynonymType annotations and omits these
# declarations, so robot filter drops the annotations.
# Now: declare the synonym-type properties on the release OWL.

INSERT DATA {
    <http://www.geneontology.org/formats/oboInOwl#SynonymTypeProperty>
        a owl:AnnotationProperty .

    <http://purl.obolibrary.org/obo/mondo#GENERATED_FROM_LABEL>
        a owl:AnnotationProperty ;
        rdfs:subPropertyOf <http://www.geneontology.org/formats/oboInOwl#SynonymTypeProperty> .

    <http://purl.obolibrary.org/obo/mondo#GENERATED>
        a owl:AnnotationProperty ;
        rdfs:subPropertyOf <http://www.geneontology.org/formats/oboInOwl#SynonymTypeProperty> .
}
