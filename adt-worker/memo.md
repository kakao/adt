# ADT 서비스 가능한 수준으로 고도화하기

* 지금까지 개발한 건 binlog 가져오는 것과 table crawling만 하고 데이터 가공이 없었음
* 이제는 지금까지 만든 것을 서비스할 수 있는 형태로 고도화 해야함

## TODO (수시로 업데이트 예정)

### Required

* 수집한 데이터를 사용자가 작성한 스크립트에 맞춰 커스텀하게 처리할 수 있는 기능 추가
* Server Application 형태로 포장
* Active-Standby 기능 추가
* Admin 개발(가급적이면 나중에 하고 싶으나 Active-Standby 설정을 위해서 필요하다고 생각함)

## 고민해볼 사항들

* 사용자 Custom Script로 어떤 언어들을 지원할 것인가?
  - ScriptEngine으로 어떤 언어를 제공할 수 있는지
    - Nashorn(Javascript, Java8 이상)
      - Java에서 Javascript 함수 호출 가능
      - Javascript에서 Java 함수(Static Method 외엔 확인 안 됨) 호출 가능
      - 참고: http://winterbe.com/posts/2014/04/05/java8-nashorn-tutorial/
    - JRuby
    - Jython (Python)
    - https://en.wikipedia.org/wiki/List_of_JVM_languages
  - ScriptEngine 말고 대체할 수 있는 건 없는지
    - javax.script.ScriptEngine 인터페이스 구현한 게 가장 표준에 가깝지 않을까 추측
  - 각 스크립트 언어별 어느 정도까지의 기능이 구현 가능한지 등등에 대해 조사 필요
    - MySQL Client 제공 or Library Import 가능 여부
    - Http Client
    - 위의 기능 둘 다 안 되면 Java에서 구현한 Method 콜 가능한지?
  - 가능하다면 커스텀 자바 클래스를 동적 로드해서 처리하는 기능이 들어갈 수도 있을 듯
  - 아니면 그냥 worker에서 Runtime.exec() 실행해서 처리해버리는 것도 방법 중 하나
    - 성능 이슈가 있을 것 같음


* admin에서 job 추가 시, worker에 어떤 방식으로 일을 할당해줄 것인지...
  - 후보1. 이미 떠있는 worker에 명령 전달(jmx or admin tool)
  - 후보2. admin이 worker용 머신에 ssh로 접속하여 job마다 별도의 worker app 실행
    - (Gordon: 각각의 job이 매번 새로운 프로세스로 작동하므로 가장 안정적일 것으로 판단함)
  - 후보3. 놀고있는 worker들이 주기적으로 job list DB에 polling

## Iter. #1 Spec.
- Admin
  - 시작/종료/재시작
  - Destination 설정 기능 최소 스펙으로...
  - 동작 상태 보기

## 그 다음에는...
- 장애 감지 및 Active-Standby 설정
- Destination 템플릿 제공
- worker 로직 업데이트 기능
