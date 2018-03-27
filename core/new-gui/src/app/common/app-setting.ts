/* tslint:disable:no-inferrable-types */
export class AppSettings {
    private static readonly SERVER_ADDRESS: string = 'http://localhost';
    private static readonly API_PORT: number = 8080;
    public static API_ENDPOINT: string = `${AppSettings.SERVER_ADDRESS}:${AppSettings.API_PORT}/api`;
}
