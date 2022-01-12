## Comparison of potential JSON Schema form libraries

We chose [angular2-json-schema-form](https://github.com/dschnelldavis/angular2-json-schema-form) to generate the form based on the json Schema. Although it has many issues, it's a good choice in the current situation.

There are also many other libraries available. Here's the metrics to compare them:

- if it's written for Angular 2+ (if it's not for Angular 2+, then if it's in pure Javascript, we want to avoid libraries for other frameworks, such as Angular 1 or React)
- popularity, how many stars on github
- maintenance and activeness, if the library author is actively maintaining it
- user-friendliness and looks of the form it generated

Here is the comparison table:

| Library                                                                                                           | Angular 2+ | Popularity | Maintenance                                                                                                                                                   | Look |
| ----------------------------------------------------------------------------------------------------------------- | ---------- | ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---- |
| Our choice: [dschnelldavis/angular2-json-schema-form](https://github.com/dschnelldavis/angular2-json-schema-form) | Yes        | 200+       | Very poor                                                                                                                                                     | Good |
| [makinacorpus/ngx-schema-form](https://github.com/makinacorpus/ngx-schema-form)                                   | Yes        | 200+       | Very poor                                                                                                                                                     | Bad  |
| [json-schema-form/angular-schema-form](https://github.com/json-schema-form/angular-schema-form)                   | Angular 1  | 2000+      | Good. The author intended to make an [Angular 2+ version](https://github.com/json-schema-form/angular-schema-form/issues/774), but progress is very very slow | Good |
| [jdorn/json-editor](https://github.com/jdorn/json-editor)                                                         | JavaScript | 4000+      | Original author no longer maintains. Primary Fork by community: [json-editor/json-editor](https://github.com/json-editor/json-editor)                         | Ok   |
| [joshfire/jsonform](https://github.com/joshfire/jsonform)                                                         | JavaScript | 1000+      | Last updated 5 years ago                                                                                                                                      | -    |
| [gitana/alpaca](https://github.com/gitana/alpaca)                                                                 | jQuer)     | 900+       | not actively maintained: 300+ issues left open                                                                                                                | Ok   |

In conclusion, among all the potential libraries:

- there's only 2 for Angular 2+, both are pooly maintained
- most of them are not actively maintained (except the angular 1 library)

We chose the Angular 2 library because it's easy to integrate and the form it generates looks good.

However, the current library blocks our way of updating to Angular 6:

- it relies on a beta version of `@angular/flex-layout`, updating it would break
- no updates for Angular 6

If our current choice cannot satisfy our need or blocked our way of updating Angular,
we are open to:

1.  switch to another library:

- `makinacorpus/ngx-schema-form`: recently added a PR to support Angular 6
- `json-schema-form/angular-schema-form`: spend time integrating Angular 1 library into our app

2.  make a fork (or use other people's fork) of the current library:

- many people have forked the library to meet their need
  https://github.com/dschnelldavis/angular2-json-schema-form/pull/230#issuecomment-383591628
