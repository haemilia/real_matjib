# Git LFS(Large File Storage) 사용하기
**git LFS 웹사이트:** [https://git-lfs.com/](https://git-lfs.com/)
## 1. git LFS를 설치한다.
- git LFS 웹사이트에서 `Download (Windows)` 버튼을 눌러 설치한다. 
- 로컬에서 다음 명령어를 실행한다. 사용자 당 한 번만 실행하면 된다.
```bash
git lfs install
```
## 2. git LFS를 사용하고 싶은 repository에서
- 현재 우리의 repository는 다른 사용자가 이미 이 단계를 진행했지만, 앞으로 다른 파일 형식도 등록하고 싶다면 다음과 같이 진행한다.
- 무료 GitHub 사용자는 최대 2GB 까지만 git LFS 트래킹이 가능하다는 점 염두에 두고 진행하도록 바란다.
```bash
git lfs track "*.db"
```
- 이렇게 실행하면 앞으로 `.db` 확장자를 가진 파일은 모두 git lfs가 트래킹한다. 
- 해당 사항은 `.gitattributes` 파일에 등록되어 있다.
- 자세한 건 스스로 알아보시도록...