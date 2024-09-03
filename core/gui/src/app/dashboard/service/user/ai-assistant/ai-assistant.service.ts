import { Injectable } from "@angular/core";
import { firstValueFrom } from "rxjs";
import { AppSettings } from "../../../../common/app-setting";
import { HttpClient } from "@angular/common/http";

export const AI_ASSISTANT_API_BASE_URL = `${AppSettings.getApiEndpoint()}/aiassistant`;

@Injectable({
  providedIn: "root",
})
export class AIAssistantService {
  constructor(private http: HttpClient) {}

  public checkAIAssistantEnabled(): Promise<string> {
    const apiUrl = `${AI_ASSISTANT_API_BASE_URL}/isenabled`;
    return firstValueFrom(this.http.get(apiUrl, { responseType: "text" }))
      .then(response => {
        const isEnabled = response !== undefined ? response : "NoAIAssistant";
        console.log(
          isEnabled === "OpenAI"
            ? "AI Assistant successfully started"
            : "No AI Assistant or OpenAI authentication key error"
        );
        return isEnabled;
      })
      .catch(() => {
        return "NoAIAssistant";
      });
  }

  public getTypeAnnotations(code: string, lineNumber: number, allcode: string): Promise<string> {
    const prompt = `
            Your task is to analyze the given Python code and provide only the type annotation as stated in the instructions.
            Instructions:
            - The provided code will only be one of the 2 situations below:
            - First situation: The input is not start with "def". If the provided code only contains variable, output the result in the format ":type".
            - Second situation: The input is start with "def". If the provided code starts with "def" (a longer line than just a variable, indicative of a function or method), output the result in the format " -> type".
            - The type should only be one word, such as "str", "int", etc.
            Examples:
            - First situation:
                - Provided code is "name", then the output may be : str
                - Provided code is "age", then the output may be : int
                - Provided code is "data", then the output may be : Tuple[int, str]
                - Provided code is "new_user", then the output may be : User
                - A special case: provided code is "self" and the context is something like "def __init__(self, username :str , age :int)", if the user requires the type annotation for the first parameter "self", then you should generate nothing.
            - Second situation: (actual output depends on the complete code content)
                - Provided code is "process_data(data: List[Tuple[int, str]], config: Dict[str, Union[int, str]])", then the output may be -> Optional[str]
                - Provided code is "def add(a: int, b: int)", then the output may be -> int
            Counterexamples:
            - Provided code is "def __init__(self, username: str, age: int)" and you generate the result:
                The result is The provided code is "def __init__(self, username: str, age: int)", so it fits the second situation, which means the result should be in " -> type" format. However, the __init__ method in Python doesn't return anything or in other words, it implicitly returns None. Hence the correct type hint would be: -> None.
                I don't want this result! The correct result you should generate is -> None for this counter case.
            Details:
            - Provided code: ${code}
            - Line number of the provided code in the complete code context: ${lineNumber}
            - Complete code context: ${allcode}
            Important: (you must follow!!)
            - For the first situation: you must return strictly according to the format ": type", without adding any extra characters. No need for an explanation, just the result : type is enough!
            - For the second situation: you return strictly according to the format " -> type", without adding any extra characters. No need for an explanation, just the result -> type is enough!
        `;
    return firstValueFrom(this.http.post<any>(`${AI_ASSISTANT_API_BASE_URL}/getresult`, { prompt }))
      .then(response => {
        if (response.choices && response.choices.length > 0) {
          return response.choices[0].message.content.trim();
        } else {
          console.error("Error from backend:", response.body);
          return "";
        }
      })
      .catch(error => {
        console.error("Request to backend failed:", error);
        return "";
      });
  }
}
