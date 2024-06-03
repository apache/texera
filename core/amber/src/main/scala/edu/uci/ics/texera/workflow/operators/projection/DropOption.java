package edu.uci.ics.texera.workflow.operators.projection;

import com.fasterxml.jackson.annotation.JsonValue;

public enum DropOption {



        keep("Keep"),
        drop("Drop"),

        ;

        private final String name;
        DropOption(String name) {
            this.name = name;
        }

        @JsonValue
        public String getName() {
            return this.name;
        }


    }


