package edu.uci.ics.texera.workflow.common.metadata.annotations;

public class UIWidget {

    public static final String UIWidgetTextArea = "{ \"widget\": {\n          \"formlyConfig\": {\n            \"type\": \"textarea\",\n            \"templateOptions\": {\n              \"autosize\": true,\n              \"autosizeMinRows\": 3\n            }\n          }\n        }\n      }";

    public static final String UIWidgetPassword = "{ \"widget\": {\n          \"formlyConfig\": {\n            \"templateOptions\": {\n              \"type\": \"password\"\n            }\n          }\n        }\n      }";

}
