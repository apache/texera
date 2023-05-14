export interface Source
  extends Readonly<{
    type: string;
    id: string;
  }> {}

export interface Metadata
  extends Readonly<{
    primary: boolean;
    source: Source;
  }> {}

export interface Photo
  extends Readonly<{
    metadata: Metadata;
    url: string;
  }> {}

export interface GooglePeopleApiResponse
  extends Readonly<{
    resourceName: string;
    etag: string;
    photos: Photo[];
  }> {}
