/* SystemJS module definition */
declare var module: NodeModule;
interface NodeModule {
  id: string;
}

import * as DeckTypings from "@danmarshall/deckgl-typings";
declare module "deck.gl" {
  export namespace DeckTypings {}
}
