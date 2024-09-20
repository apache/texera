<h1 align="center">Texera - Collaborative Data Analytics Using Workflows.</h1>

<p align="center">
  <img src="core/gui/src/assets/logos/full_logo_small.png" alt="texera-logo" width="192px" height="109px"/>
  <br>
  <i>Texera supports scalable computation and enables advanced AI/ML techniques.</i>
  <br>
  <i>"Collaboration" is a key focus, and we enable an experience similar to Google Docs, but for data analytics. </i>
  <br>
  
  <h4 align="center">
    <a href="https://youtu.be/2gfPUZNsoBs">Demo Video</a>
    |
    <a href="https://texera.github.io/blog/">Blogs</a>
    |
    <a href="https://github.com/Texera/texera/wiki/Getting-Started">Getting Started</a>
    <br>
  </h4>
  
</p>
</p>
<p align="center">
  <img alt="Static Badge" src="https://img.shields.io/badge/Users-332-blue">
  <img alt="Static Badge" src="https://img.shields.io/badge/Projects-86-blue">
  <img alt="Static Badge" src="https://img.shields.io/badge/Workflows-2,481-blue">
  <img alt="Static Badge" src="https://img.shields.io/badge/Executions-51K-blue">
  <img alt="Static Badge" src="https://img.shields.io/badge/Workflow_Versions-357K-blue">
  <img alt="Static Badge" src="https://img.shields.io/badge/Deployments-7-blue">
  <img alt="Static Badge" src="https://img.shields.io/badge/Largest_Deployment-100_nodes,_400_cores-green">
</p>

<!---
(Orignal intro paragraph, commented out)
## Texera

Texera is a system to support collaborative, ML-centric data analytics as a cloud-based service using GUI-based workflows. It supports scalable computation with a parallel backend engine, and enables advanced AI/ML techniques. "Collaboration" is a key focus, and we want to enable an experience similar to existing services such as Google Docs, but for data analytics, especially for people with different backgrounds, including IT developers and domain scientists with limited programming background.
-->

## Motivation

* Many data analysts need to spend a significant amount of effort on low-level computation to do data wrangling and preparation, and want to use latest AI/ML techniques. These tasks are especially tough for non-IT users. 

* Many workflow-based analysis systems are not parallel, making them not capable of dealing with big data sets. 

* Cloud-based services and technologies have emerged and advanced significantly in the past decade. Emerging browser-based techniques make it possible to develop powerful browser-based interfaces, which also benefit from high-speed networks.

* Existing big data systems support little interaction during the execution of a long running job, making them hard to manage once they are started.

## Goals

* Provide data analytics as cloud services;
* Provide a browser-based GUI to form a workflow without writing code;
* Allow non-IT people to do data analytics;
* Support collaborative data analytics;
* Allow users to interact with the execution of a job;
* Support huge volumes of data efficiently.

## Sample Workflow

The following is a workflow formulated using the Texera GUI in a Web browser, which consists of operators such as regex search, sentiment analysis, user-defined function (UDF) in Python, and visualization.

![Sample Texera Workflow](https://user-images.githubusercontent.com/12926365/171459157-1792971d-a31f-49e7-ab98-6f3b9ead9f5b.png)

## Publications (Computer Science):
* (8/2024) Pasta: A Cost-Based Optimizer for Generating Pipelining Schedules for Dataflow DAGs, Xiaozhen Liu, Yicong Huang, Xinyuan Lin, Avinash Kumar, Sadeem Alsudais, and Chen Li, to appear in SIGMOD 2025.
* (7/2024) Texera: A System for Collaborative and Interactive Data Analytics Using Workflows, Zuozhi Wang, Yicong Huang, Shengquan Ni, Avinash Kumar, Sadeem Alsudais, Xiaozhen Liu, Xinyuan Lin, Yunyan Ding and Chen Li, to appear in VLDB 2024, Scalable Data Science track [PDF](https://www.vldb.org/pvldb/vol17/p3580-wang.pdf), [Slides](https://chenli.ics.uci.edu/files/vldb2024-texera-presentation.pdf).
* (3/2024) Demonstration of Udon: Line-by-line Debugging of User-Defined Functions in Data Workflows, Yicong Huang, Zuozhi Wang, and Chen Li, in SIGMOD 2024, **Best Demo Runner Up Award🏆**, [PDF](https://dl.acm.org/doi/10.1145/3626246.3654756). 
* (2/2024) Data Science Tasks Implemented with Scripts versus GUI-Based Workflows: The Good, the Bad, and the Ugly, Alexander K Taylor, Yicong Huang, Junheng Hao, Xinyuan Lin, Xiusi Chen, Wei Wang, and Chen Li, in DataPlat Workshop at ICDE 2024 [PDF](https://ieeexplore.ieee.org/abstract/document/10555112). 
* (8/2023) Building a Collaborative Data Analytics System: Opportunities and Challenges, Zuozhi Wang, Chen Li, in Tutorial at VLDB 2023 [PDF](https://www.vldb.org/pvldb/vol16/p3898-wang.pdf), [Slides](https://chenli.ics.uci.edu/files/vldb2023-texera-tutorial.pdf).
* (8/2023) Udon: Efficient Debugging of User-Defined Functions in Big Data Systems with Line-by-Line Control, Yicong Huang, Zuozhi Wang, and Chen Li, in SIGMOD 2024 [PDF](https://dl.acm.org/doi/10.1145/3626712).
* (8/2023) Improving Iterative Analytics in GUI-Based Data-Processing Systems with Visualization,
  Version Control, and Result Reuse, Sadeem Alsudais, Ph.D. Thesis [PDF](https://sadeemsaleh.github.io/Sadeem_phd_thesis.pdf).
* (7/2023) Using Texera to Characterize Climate Change Discussions on Twitter During Wildfires, Shengquan Ni, Yicong Huang, Jessie W. Y. Ko, Alexander Taylor, Xiusi Chen, Avinash Kumar, Sadeem Alsudais, Zuozhi Wang, Xiaozhen Liu, Wei Wang, Suellen Hopfer, and Chen Li, in Data Science Day at KDD 2023.
* (7/2023) Raven: Accelerating Execution of Iterative Data Analytics by Reusing Results of Previous Equivalent Versions, Sadeem Alsudais, Avinash Kumar, and Chen Li, in HILDA Workshop at SIGMOD 2023 [PDF](https://dl.acm.org/doi/10.1145/3597465.3605219).
* (6/2023) Texera: A System for Collaborative and Interactive Data Analytics Using Workflows, Zuozhi Wang, Ph.D. Thesis [PDF](https://zuozhiw.github.io/Zuozhi_Wang_UCI_PhD_Thesis.pdf).
* (12/2022) Towards Interactive, Adaptive and Result-aware Big Data Analytics, Avinash Kumar, Ph.D. Thesis [PDF](https://arxiv.org/abs/2212.07096).
* (9/2022) Fries: Fast and Consistent Runtime Reconfiguration in Dataflow Systems with Transactional Guarantees, Zuozhi Wang, Shengquan Ni, Avinash Kumar, and Chen Li, in VLDB 2023 [PDF](https://www.vldb.org/pvldb/vol16/p256-wang.pdf), [Slides](https://chenli.ics.uci.edu/files/vldb2023-fries.pdf).
* (7/2022) Drove: Tracking Execution Results of Workflows on Large Datasets, Sadeem Alsudais, in the Ph.D. Workshop at VLDB 2022 [PDF](http://ceur-ws.org/Vol-3186/paper_10.pdf).
* (6/2022) Demonstration of Accelerating Machine Learning Inference Queries with Correlative Proxy Models, Zhihui Yang, Yicong Huang, Zuozhi Wang, Feng Gao, Yao Lu, Chen Li, and X. Sean Wang, in VLDB 2022 [PDF](https://www.vldb.org/pvldb/vol15/p3734-yang.pdf).
* (6/2022) Demonstration of Collaborative and Interactive Workflow-Based Data Analytics in Texera, Xiaozhen Liu, Zuozhi Wang, Shengquan Ni, Sadeem Alsudais, Yicong Huang, Avinash Kumar, and Chen Li, in VLDB 2022 [PDF](https://www.vldb.org/pvldb/vol15/p3738-liu.pdf), [Demo Video](https://youtu.be/2gfPUZNsoBs).
* (4/2022) Optimizing Machine Learning Inference Queries with Correlative Proxy Models, Zhihui Yang, Zuozhi Wang, Yicong Huang, Yao Lu, Chen Li, and X. Sean Wang, in VLDB 2022 [PDF](https://www.vldb.org/pvldb/vol15/p2032-yang.pdf).
* (7/2020) Demonstration of Interactive Runtime Debugging of Distributed Dataflows in Texera, Zuozhi Wang, Avinash Kumar, Shengquan Ni, and Chen Li, in VLDB 2020 [PDF](http://www.vldb.org/pvldb/vol13/p2953-wang.pdf), [Video](https://www.youtube.com/watch?v=SP-XiDADbw0), [Slides](https://docs.google.com/presentation/d/14U6RPZfeb8Ho0aO2HsCSc8lRs6ul6AxEIm5gpjeVUYA/edit?usp=sharing).
* (1/2020) Amber: A Debuggable Dataflow system based on the Actor Model, Avinash Kumar, Zuozhi Wang, Shengquan Ni, and Chen Li, VLDB 2020 [PDF](http://www.vldb.org/pvldb/vol13/p740-kumar.pdf), [Video](https://www.youtube.com/watch?v=T5ShFRfHmgI), [Slides](https://docs.google.com/presentation/d/1v8G9lDmfv4Ff2YWyrGfo_9iMQVF4N8a-4gO4H-K6rCk/edit?usp=sharing).
* (4/2017) A Demonstration of TextDB: Declarative and Scalable Text Analytics on Large Data Sets, Zuozhi Wang, Flavio Bayer, Seungjin Lee, Kishore Narendran, Xuxi Pan, Qing Tang, Jimmy Wang, and Chen Li, [ICDE 2017](http://icde2017.sdsc.edu/), **Best Demo award**, [PDF](https://chenli.ics.uci.edu/files/icde2017-textdb-demo.pdf), [Video](https://github.com/Texera/texera/wiki/Video).


## Publications (Interdisciplinary):
* (7/2024) Brain image data processing using collaborative data workflows on Texera, Yunyan Ding, Yicong Huang, Pan Gao, Andy Thai, Atchuth Naveen Chilaparasetti, M. Gopi, Xiangmin Xu, and Chen Li, in Frontiers Neural Circuits [PDF](https://doi.org/10.3389/fncir.2024.1398884).
* (1/2024) Wording Matters: the Effect of Linguistic Characteristics and Political Ideology on Resharing of COVID-19 Vaccine Tweets, Judith Borghouts, Yicong Huang, Suellen Hopfer, Chen Li, and Gloria Mark, in TOCHI 2024 [PDF](https://dl.acm.org/doi/pdf/10.1145/3637876).
* (1/2024) How the experience of California wildfires shape Twitter climate change framings, Jessie W. Y. Ko, Shengquan Ni, Alexander Taylor, Xiusi Chen, Yicong Huang, Avinash Kumar, Sadeem Alsudais, Zuozhi Wang, Xiaozhen Liu, Wei Wang, Chen Li, and Suellen Hopfer, in Climatic Change 2024 [PDF](https://link.springer.com/content/pdf/10.1007/s10584-023-03668-0.pdf).
* (11/2023) The Marketing and Perceptions of Non-Tobacco Blunt Wraps on Twitter, Joshua U. Rhee, Yicong Huang, Aurash J. Soroosh, Sadeem Alsudais, Shengquan Ni, Avinash Kumar, Jacob Paredes, Chen Li, and David S. Timberlake, in Substance Use & Misuse 2023 [PDF](https://www.tandfonline.com/doi/epdf/10.1080/10826084.2023.2280572?needAccess=true).
* (3/2023) Understanding underlying moral values and language use of COVID-19 vaccine attitudes on twitter, Judith Borghouts, Yicong Huang, Sydney Gibbs, Suellen Hopfer, Chen Li, and Gloria Mark, in PNAS Nexus 2023 [PDF](https://academic.oup.com/pnasnexus/article-pdf/2/3/pgad013/49435858/pgad013.pdf).
* (10/2022) Public Opinions toward COVID-19 Vaccine Mandates: A Machine Learning-based Analysis of U.S. Tweets, Yawen Guo, Jun Zhu, Yicong Huang, Lu He, Changyang He, Chen Li, and Kai Zheng, in AMIA 2022 [PDF](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC10148373/pdf/1066.pdf).
* (9/2021) The Social Amplification and Attenuation of COVID-19 Risk Perception Shaping Mask Wearing Behavior: A Longitudinal Twitter Analysis, Suellen Hopfer, Emilia J. Fields, Yuwen Lu, Ganesh Ramakrishnan, Ted Grover, Quishi Bai, Yicong Huang, Chen Li, and Gloria Mark, in PLOS ONE 2021 [PDF](https://journals.plos.org/plosone/article?id=10.1371/journal.pone.0257428).
* (4/2021) Why Do People Oppose Mask Wearing? A Comprehensive Analysis of US Tweets During the COVID-19 Pandemic, Lu He, Changyang He, Tera Leigh Reynolds, Qiushi Bai, Yicong Huang, Chen Li, Kai Zheng, and Yunan Chen, in JAMIA 2021 [PDF](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC7989302/pdf/ocab047.pdf).

## Education

* [Data Science for All](https://ds4all.ics.uci.edu/) (an NSF-funded summer program to teach high-school students data science and AI/ML)
* [ICS 80](https://canvas.eee.uci.edu/courses/63639/pages/syllabus) (a Spring 2024 course at UCI, teaching 42 undergraduates, most of whom are not computer science majors, to learn data science and AI/ML)

## Videos
* [dkNET Webinar "Texera: A Scalable Cloud Computing Platform for Sharing Data & Workflow-based Analysis" 04/26/2024](https://www.youtube.com/watch?v=B81iMFS5fPc)
* [Texera demo in VLDB 2020](https://www.youtube.com/watch?v=SP-XiDADbw0)
* [Amber engine presentation in VLDB 2020](https://www.youtube.com/watch?v=T5ShFRfHmgI)
* See [Texera in action](https://www.youtube.com/watch?v=NXfynBUwdVg). 

## Getting Started

* For users, visit [Guide to Use Texera](https://github.com/Texera/texera/wiki/Getting-Started).
* For developers, visit [Guide to Develop Texera](https://github.com/Texera/texera/wiki/Guide-for-Developers).

Texera was formally known as "TextDB" before August 28, 2017.

## Instructions for VLDB 2022 Demo Paper

To try our collaborative data analytics in _Demonstration of Collaborative and Interactive Workflow-Based Data Analytics in Texera_, visit [https://github.com/Texera/texera/wiki/Instructions-for-VLDB-2022-Demo](https://github.com/Texera/texera/wiki/Instructions-for-VLDB-2022-Demo).

## Acknowledgements

This project is supported by the <a href="http://www.nsf.gov">National Science Foundation</a> under the awards [IIS-1745673](https://www.nsf.gov/awardsearch/showAward?AWD_ID=1745673), [IIS-2107150](https://www.nsf.gov/awardsearch/showAward?AWD_ID=2107150), AWS Research Credits, and Google Cloud Platform Education Programs.

* <a href="https://www.niddk.nih.gov/"><img src="https://github.com/Texera/texera/assets/17627829/d279897a-3efb-41c1-b2d3-8fd20c800ad7" alt="NIH NIDDK" height="30"/></a> This project is supported by an <a href="https://reporter.nih.gov/project-details/10818244">NIH NIDDK</a> award.


* <a href="http://www.yourkit.com"><img src="https://www.yourkit.com/images/yklogo.png" alt="Yourkit" height="30"/></a>  [Yourkit](https://www.yourkit.com/) has given an open source license to use their profiler in this project.

## Citation
Please cite Texera as 
```
@article{10.14778/3681954.3682022,
  author = {Wang, Zuozhi and Huang, Yicong and Ni, Shengquan and Kumar, Avinash and Alsudais, Sadeem and Liu, Xiaozhen and Lin, Xinyuan and Ding, Yunyan and Li, Chen},
  title = {Texera: A System for Collaborative and Interactive Data Analytics Using Workflows},
  year = {2024},
  issue_date = {July 2024},
  publisher = {VLDB Endowment},
  volume = {17},
  number = {11},
  issn = {2150-8097},
  url = {https://doi.org/10.14778/3681954.3682022},
  doi = {10.14778/3681954.3682022},
  journal = {Proc. VLDB Endow.},
  month = {aug},
  pages = {3580–3588},
  numpages = {9}
}
```
