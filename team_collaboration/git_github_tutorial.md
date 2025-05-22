# 로컬에서 git 사용하기
## 1단계: 윈도우에 git 설치
1. **git 공식 웹사이트:** [https://git-scm.com/download/win](https://git-scm.com/download/win)에 접속
2. **git 다운로드:** 최신 버전의 git을 다운로드하여 설치
3. **git 설치:** 설치 과정에서 다양한 설정 옵션이 제시되지만, 특별한 요구 사항이 없다면 **기본 설정**을 유지하고 "Next" 버튼을 클릭하여 설치를 진행
4. **설치 완료 확인:** 설치가 완료되면 "Finish" 버튼을 클릭하여 설치 프로그램을 종료

설치가 정상적으로 완료되었는지 확인하기 위해 CMD/Powershell를 실행하고 다음 명령어를 입력한다.

```
git --version
```

정상적으로 설치되었다면 git의 버전 정보가 출력될 것이다.

```
git version 2.XX.X.windows.1
```

## 2단계: git 사용자 정보 설정 (`config`)

git은 각 commit에 대한 작성자를 기록하기 위해 사용자 이름과 이메일 주소를 설정해야 한다. 터미널(eg. CMD, Git Bash)을 실행하고 다음 명령어를 입력하여 사용자 정보를 설정한다.

```
git config --global user.name "사용자 본인의 이름"
```

```
git config --global user.email "사용자 본인의 이메일 주소"
```

**주의:** `--global` 옵션은 현재 컴퓨터의 모든 git 저장소에 해당 설정을 적용하는 것이다. 특정 프로젝트에 대해 다른 이름이나 이메일 주소를 사용해야 하는 경우, `--local` 옵션을 사용하거나 해당 저장소 디렉토리에서 `--global` 옵션 없이 명령어를 실행하면 된다.

설정이 제대로 되었는지 확인하려면 다음 명령어를 각각 입력한다.

```
git config --get user.name
```

```
git config --get user.email
```

각각 설정한 사용자 이름과 이메일 주소가 출력되면 설정이 완료된 것이다.

## 3단계: 로컬 저장소에서 변경 사항 추적

1. **프로젝트 폴더로 이동:** git으로 관리하고자 하는 프로젝트 폴더로 CMD 또는 Git Bash를 사용하여 이동한다. 예를 들어, 프로젝트 폴더명이 `myproject`인 경우 다음과 같이 입력한다.
    
    ```
    cd myproject
    ```
    
2. **git 저장소 초기화:** 해당 폴더를 git 저장소로 만들기 위해 다음 명령어를 실행한다.
    
    ```
    git init
    ```
    
    성공적으로 초기화되면 다음과 유사한 메시지가 출력된다.
    
    ```
    Initialized empty Git repository in C:/Users/사용자이름/myproject/.git/
    ```
    
    `.git`이라는 숨겨진 폴더가 생성된 것을 확인할 수 있다. 이 폴더는 git의 모든 버전 관리 정보를 저장하는 곳이다.
    
3. **파일 추가 (`git add`):** 변경 사항을 추적하고자 하는 파일을 git의 스테이징 영역에 추가한다. 예를 들어, `myfile.txt` 파일을 추가하려면 다음 명령어를 입력한다.
    
    ```
    git add myfile.txt
    ```
    
    현재 폴더 및 하위 폴더의 모든 변경된 파일을 한 번에 스테이징 영역에 추가하려면 다음 명령어를 사용한다.
    
    ```
    git add .
    ```
    
4. **커밋 (`git commit`):** 스테이징 영역에 있는 변경 사항을 로컬 git 저장소에 기록하는 행위를 커밋이라고 한다. 각 커밋은 변경 사항에 대한 설명을 포함해야 한다.
    
    ```
    git commit -m "Initial commit: myfile.txt 파일 생성 및 내용 추가"
    ```
    
    `-m` 옵션 뒤에 커밋 메시지를 작성한다. 커밋 메시지는 변경 사항의 내용과 목적을 명확하게 기술하는 것이 중요하다.
    
5. **로그 확인 (`git log`):** 지금까지의 커밋 기록을 확인하려면 다음 명령어를 사용한다.
    
    ```
    git log
    ```
    
    커밋 기록에는 각 커밋의 고유 식별자(SHA-1 해시), 작성자, 작성 일시, 커밋 메시지 등이 표시된다.
    
    다양한 옵션을 사용하여 로그 정보를 상세하게 확인할 수 있다.
    
    - `git log --oneline`: 각 커밋을 한 줄로 간략하게 표시한다.
    - `git log --graph --oneline --decorate --all`: 브랜치 구조와 함께 커밋 그래프를 시각적으로 표현한다.
    - `git log -p`: 각 커밋에서 변경된 내용의 상세 diff 정보를 함께 표시한다.
    - `git log --since="2 days ago"`: 최근 2일 동안의 커밋만 표시한다.
6. **변경 사항 되돌리기 (`git revert`):** 특정 커밋의 변경 사항을 취소하는 새로운 커밋을 생성한다. 이전 커밋 기록은 유지된다. 되돌리고자 하는 커밋의 해시 ID를 사용한다.
    
    ```
    git revert <커밋 해시 ID>
    ```
    
    예를 들어, 커밋 해시 ID가 `a1b2c3d4`인 경우 다음과 같이 입력한다.
    
    ```
    git revert a1b2c3d4
    ```
    
    해당 명령어를 실행하면 해당 커밋의 변경 사항을 취소하는 새로운 커밋 메시지 작성을 요청하는 편집기가 나타난다. 메시지를 작성하고 저장하면 되돌리기 커밋이 생성된다.
    
7. **변경 사항 되돌리기 (`git reset`):** 저장소의 상태를 특정 커밋으로 되돌린다. `revert`와 달리 이전 커밋 기록을 변경할 수 있으므로 주의하여 사용해야 한다.
    
    - `git reset --soft <커밋 해시 ID>`: HEAD와 현재 브랜치를 지정된 커밋으로 이동시키지만, 스테이징 영역과 작업 디렉토리는 변경하지 않는다. 변경 사항은 "Changes to be committed" 상태로 유지된다.
    - `git reset --mixed <커밋 해시 ID>` (기본 옵션): HEAD와 현재 브랜치를 지정된 커밋으로 이동시키고, 스테이징 영역의 변경 사항을 취소한다. 작업 디렉토리의 파일은 그대로 유지되며, 변경 사항은 "Changes not staged for commit" 상태로 표시된다.
    - `git reset --hard <커밋 해시 ID>`: HEAD, 현재 브랜치, 스테이징 영역, 작업 디렉토리 모두를 지정된 커밋 상태로 되돌린다. **이 옵션은 작업 내용을 영구적으로 삭제할 수 있으므로 매우 신중하게 사용해야 한다.**
    
    예를 들어, 바로 이전 커밋으로 되돌리려면 다음 명령어를 입력한다.
    
    ```
    git reset --hard HEAD^
    ```
    

## 4단계: 브랜치 이해 및 사용

브랜치는 독립적인 작업 환경을 제공하여 여러 기능을 동시에 개발하거나 실험적인 수정사항을 격리하는 데 사용된다.

1. **브랜치 목록 확인 (`git branch`):** 로컬 저장소의 브랜치 목록을 표시한다. 현재 활성화된 브랜치 앞에는 `*` 기호가 표시된다.
    
    ```
    git branch
    ```
    
    출력 예시:
    
    ```
      main
    * develop
      feature-x
    ```
    
2. **새로운 브랜치 생성 (`git branch <브랜치 이름>`):** 새로운 브랜치를 생성한다. 이 명령어는 단순히 새로운 브랜치를 생성할 뿐, `HEAD`가 머무르고 있는 브랜치는 변경되지 않는다.
    
    ```
    git branch new-feature
    ```
    
3. **브랜치 이동 (`git checkout <브랜치 이름>`):** 생성된 브랜치로 전환하여 작업을 시작한다.
    
    ```
    git checkout new-feature
    ```
    
    성공적으로 브랜치가 전환되면 다음과 유사한 메시지가 출력된다.
    
    ```
    Switched to branch 'new-feature'
    ```
    
4. **HEAD의 개념:** `HEAD`는 현재 작업 중인 브랜치의 최신 커밋을 가리키는 포인터이다. `checkout` 명령어를 사용하여 다른 브랜치로 이동하면 `HEAD` 또한 해당 브랜치의 최신 커밋을 가리키도록 변경된다.
    
# GitHub와 연동하여 협업하기
## GitHub와 연동하기

GitHub는 웹 기반의 깃 저장소 호스팅 서비스이다. 로컬 저장소의 내용을 GitHub와 동기화하여 협업하고 백업할 수 있다.

1. **GitHub에 새 저장소 생성:** GitHub 웹사이트 ([https://github.com/](https://github.com/))에 접속하여 새로운 저장소를 생성한다. 저장소 이름을 설정하고, 필요에 따라 설명을 추가할 수 있다. "Initialize this repository with:" 옵션은 선택하지 않고 "Create repository" 버튼을 클릭한다.
    
2. **원격 저장소 추가 (`git remote add origin <GitHub 저장소 URL>`):** 로컬 저장소와 GitHub 저장소를 연결한다. `<GitHub 저장소 URL>`은 GitHub에서 생성한 저장소의 URL이다.
    
    ```
    git remote add origin https://github.com/보통본인계정/저장소이름.git
    ```
    
    `origin`은 원격 저장소의 기본 이름으로 사용된다.
    
3. **원격 저장소 복제 (`git clone <GitHub 저장소 URL>`):** 이미 GitHub에 존재하는 저장소를 로컬 환경으로 가져오는 방법이다. 새로운 폴더를 생성할 필요 없이 지정된 URL의 저장소를 통째로 복사해 온다.
    
    ```
    git clone https://github.com/누군가의계정/협업저장소.git
    ```
    
---
## 제안하는 협업 워크플로우

- 일반적인 git과 GitHub를 활용한 협업 워크플로우
- 제안할 다른 사항이 있거나 변경하고 싶은 게 있다면 언제든지 환영합니다~

1. **로컬 `main` 브랜치 업데이트:** 원격 저장소의 최신 변경 사항을 로컬 `main` 브랜치에 반영한다.
    
    ```
    git checkout main
    git pull origin main
    ```
    
    - `git checkout main` 명령어는 현재 작업 브랜치를 `main`으로 전환
    - `git pull origin main` 명령어는 `origin` 원격 저장소의 `main` 브랜치로부터 변경 사항을 다운로드하여 현재 로컬 `main` 브랜치에 병합
    
    원격 저장소에 새로운 커밋이 존재했다면 다음과 유사한 출력이 나타날 수 있다.
    
    ```
    remote: Enumerating objects: 5, done.
    remote: Counting objects: 100% (5/5), done.
    remote: Compressing objects: 100% (3/3), done.
    remote: Total 3 (delta 0), reused 0 (delta 0), pack-reused 0
    Unpacking objects: 100% (3/3), 688 bytes | 114.00 KiB/s, done.
    From https://github.com/본인계정/저장소이름
     * branch            main       -> FETCH_HEAD
       a1b2c3d..e4f5g6h main       -> origin/main
    Updating a1b2c3d..e4f5g6h
    Fast-forward
     README.md | 1 +
     1 file changed, 1 insertion(+)
    ```
    
2. **기능 브랜치 생성:** 새로운 기능을 개발하거나 버그를 수정하기 위해 격리된 작업 환경인 브랜치를 생성한다. 예를 들어 `hi`라는 이름의 브랜치를 생성한다.
    
    ```
    git branch hi
    ```
    
3. **기능 브랜치로 이동:** 생성한 `hi` 브랜치로 전환한다.
    
    ```
    git checkout hi
    ```
    
    출력:
    
    ```
    Switched to branch 'hi'
    ```
    
    이후의 모든 수정 작업은 `hi` 브랜치에서 진행한다. `main` 브랜치는 항상 안정적인 상태를 유지하는 것이 권장된다.
    
    **3.1. 기능 브랜치에 메인 브랜치의 변경 사항 반영하기:** 모든 단계에서 필요한 건 아니지만, 메인 브랜치의 상태를 우리의 주된 작업 브랜치 `hi`에서도 사용할 수 있으려면 이렇게 하면 된다.
    ```bash
    git merge main
    ```
    >이건 우리의 작업 브랜치에서 사용하는 명령! 
    
4. **변경 사항 커밋:** 필요한 수정을 완료한 후, 변경된 파일을 스테이징하고 커밋한다.
    
    ```
    git add .
    git commit -m "feat: 새로운 기능 구현"
    ```
    
5. **원격 저장소에 브랜치 푸시:** 로컬 `hi` 브랜치의 커밋들을 GitHub의 `origin` 저장소에 있는 `hi` 브랜치로 업로드한다.
    
    ```
    git push origin hi
    ```
    
    처음으로 새로운 브랜치를 원격 저장소에 푸시할 때는 다음과 유사한 출력이 나타날 수 있다.
    
    ```
    Enumerating objects: 3, done.
    Counting objects: 100% (3/3), done.
    Delta compression using up to 8 threads
    Compressing objects: 100% (2/2), done.
    Writing objects: 100% (3/3), 298 bytes | 99.00 KiB/s, done.
    Total 3 (delta 0), reused 0 (delta 0), pack-reused 0
    remote:
    remote: Create a pull request for 'hi' on GitHub by visiting:
    remote:      https://github.com/본인계정/저장소이름/pull/new/hi
    remote:
    To https://github.com/본인계정/저장소이름.git
     * [new branch]      hi -> hi
    ```
    
    출력 메시지에 GitHub에서 Pull Request를 생성할 수 있는 링크가 제시된다.
    
6. **GitHub에서 Pull Request 생성:** GitHub 웹사이트로 이동하여 해당 저장소를 확인한다. 방금 푸시한 `hi` 브랜치에 대한 Pull Request를 생성하라는 알림이 표시될 것이다.
    
    - "Compare & pull request" 버튼을 클릭한다.
    - Pull Request의 제목과 설명을 작성한다. 변경 사항의 목적과 내용을 명확하게 기술하는 것이 중요하다.
    - "Create pull request" 버튼을 클릭하여 Pull Request를 생성한다.
7. **코드 리뷰:** 다른 협업자가 Pull Request의 내용과 코드 변경 사항을 검토한다. 문제가 없다면 긍정적인 의견을 댓글로 남긴다.
    
8. **Pull Request 승인 및 병합:** 브랜치 책임자 (수정 사항을 커밋하고 푸시한 사람)는 다른 협업자의 긍정적인 평가를 확인한 후, 자신의 변경 사항에 오류가 없는지 다시 한번 확인한다. 이상이 없다면 GitHub 웹사이트에서 Pull Request를 승인(Merge)한다. 병합 옵션을 선택할 수 있으며, 일반적으로 "Create a merge commit" 또는 "Squash and merge"를 사용한다. 병합 후에는 해당 기능 브랜치(`hi`)를 원격 repository에서 삭제한다.
    
9. **로컬 `main` 브랜치 업데이트 (1로 다시 돌아가 반복):** 모든 협업자는 다시 로컬 `main` 브랜치로 전환하여 최신 변경 사항을 가져온다.
    
    ```
    git checkout main
    git pull origin main
    ```
    
10. **로컬 기능 브랜치 관리 (2, 3을 다시 실행할 때 고려할 점):** 필요하다면 로컬의 `hi` 브랜치를 삭제하고 다시/새롭게 생성하거나, 단순히 기존 브랜치를 `checkout`하여 다음 작업을 진행할 수 있다.
    
    - 로컬 브랜치 삭제: `git branch -d hi` (병합된 브랜치 삭제) 또는 `git branch -D hi` (병합되지 않은 브랜치 강제 삭제, **주의하여 사용**)
    - 기존 로컬 브랜치 사용: `git checkout hi`