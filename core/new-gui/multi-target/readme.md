# Multi-Target <sub><sub><sup><sub><sup><sup>FOR WHEN NG SERVE ISN'T ENOUGH</sup></sub></sub></sub>

###### Version 0.1 / 2020-05-08

## Custom Angular Build
###### Modify the angular build process using custom builders

Edit Angular build targets in `./new-gui/angular.json`

This node module contains two custom builders:
 - ***multi-target:multi-target***
 	- Run multiple build targets at once
  - ex: run ng build and then ng serve
 	- options:
 		- *string[]* targets: build targets
 		- *boolean* sequential:  run targets sequentially or in parallel
 		- *boolean* race:  run targets in parallel until 1st one finishes


 - ***multi-target:cmd-target***  
 	- Run a shell command 
  - ex: 'npm install'
	- options:  
		- *string* cmd: command
		- *boolean* daemon: true to hide output
		- *boolean* detached:
			- execute in detached shell
            - cmd is assumed to run successfully
			- can be killed later
		- *boolean* kill: kill running cmd-task with matching cmd
		- *boolean* killChildren: kill child processes of cmd-task
			- *platform dependent*. working on Linux|Windows. 

Example angular.json :
```
{
  "projects": {
    "texera-gui": {
      "architect": {
        
        // This is the builtin "build" build target
        "build": {
          "builder": "@angular-devkit/build-angular:browser",
          "options": {
            ...
          },
          "configurations": {
            ...
          }
        }
        
        // This is an example of a cmd-target.
        "fullstack": {
          "builder": "multi-target:cmd-target",
          "options": {
            "cmd": "cd .. && java -jar ./web/target/web-0.1.0.jar server ./conf/web-config.yml",
            "detached": true
          }
        },
        
        // This is an example of a multi-target. It references the above cmd-target
        "e2e": {
          "builder": "multi-target:multi-target",
          "options": {
            "targets": [
              "texera-gui:build:production",
              "texera-gui:fullstack"
            ],
            "sequential": true
          },
        }
      }
    }
  }
}
```
## Building This Module
Run `npm run build` to build the module. build artifacts will appear in /dist

## Installing This Module
###### For the new-gui project

From /new-gui/ run `npm install ./multi-target -D` to install multi-target as a devDependency
