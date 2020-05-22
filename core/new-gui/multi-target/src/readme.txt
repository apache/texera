This module defines two angular builders:
  multi-target
  cmd-target

BUILDERS. What Are They?
  builders are functions that can be invoked via the angular command line interface
    ex: ng run <project-name>:<build-target-name>:[optional-config-name]'

  builders are composed of a js function and a json schema defining the function's inputs. (called options)

  said function and schema reside in a node module, along with the module's package.json and builders.json

  package.json references builders.json which in turn references each builder/schema pair.

  builders are then invoked in your project's angular.json, where the function's inputs are also defined

  finally, builders are executed when you run "ng run <your-project-name>:<your-build-target-name>"

    Architect (part of angular) then reads angular.json, looking for a build-target matching <your-project-name>:<your-build-target-name>

    Architect finds said build-target, which includes a builder

    Architect then finds and schedules builder, and feeds it options included in build-target

    builder then executes and returns

    Architect exits, and the shell command is completed
