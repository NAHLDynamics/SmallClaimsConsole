using Microsoft.Xrm.Sdk;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security;
using Microsoft.Xrm.Sdk.Query;
using System.IO;
namespace SmallClaimsConsole
{
    public class SharepointDocuments
    {

        public IOrganizationService service;
        public DynamicHelpers helpers;
        public SharepointDocuments(IOrganizationService _service, DynamicHelpers _helpers)
        {
            this.service = _service;
            this.helpers = _helpers;
        }

        public void CreateDocumentProcessor(EntityReference MatterLookup, string Subject, string Filename, string DocumentBody, string MimeType)
        {

            Entity DocumentProcessor = new Entity("ptl_documentprocessor");
            DocumentProcessor["ptl_matter"] = new EntityReference("incident", MatterLookup.Id);
            DocumentProcessor["ptl_description"] = Subject;
            DocumentProcessor["ptl_folder"] = "Correspondence";
            DocumentProcessor["ptl_filename"] = Filename;

            var NewDocProcessorRecord = service.Create(DocumentProcessor);

            CreateRecordAttachment(NewDocProcessorRecord, Filename, Filename, DocumentBody, MimeType, "ptl_documentprocessor");

            DocumentProcessor = service.Retrieve("ptl_documentprocessor", NewDocProcessorRecord, new ColumnSet("statecode", "statuscode"));
            DocumentProcessor["statecode"] = new OptionSetValue(1);
            DocumentProcessor["statuscode"] = new OptionSetValue(2);
            service.Update(DocumentProcessor);

        }

        public void CreateRecordAttachment(Guid ObjectID, string Subject, string Filename, string DocumentBody, string MimeType, string ParentEntityName)
        {

            Entity note = new Entity("annotation");
            note["subject"] = Subject;
            note["filename"] = Filename;
            note["documentbody"] = DocumentBody;
            note["mimetype"] = MimeType;
            note["objectid"] = new EntityReference(ParentEntityName, ObjectID);

            Guid newnote = service.Create(note);

        }

    }
}
