@echo off
chcp 65001 >nul 2>&1
title ETL Sun Sejour — Pipeline

echo.
echo =====================================================
echo   ETL Sun Sejour — Lancement du Pipeline
echo =====================================================
echo.

REM Verifier que Docker est installe
where docker >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERREUR] Docker n'est pas installe sur ce PC.
    echo.
    echo Veuillez installer Docker Desktop :
    echo   https://www.docker.com/products/docker-desktop/
    echo.
    pause
    exit /b 1
)

REM Verifier que Docker est demarre
docker info >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERREUR] Docker Desktop n'est pas demarre.
    echo.
    echo Ouvrez Docker Desktop, attendez qu'il soit pret,
    echo puis relancez ce fichier.
    echo.
    pause
    exit /b 1
)

echo [1/2] Preparation de l'environnement Python...
echo       (premiere fois : peut prendre 2-3 minutes)
echo.
docker compose build etl
if %errorlevel% neq 0 (
    echo [ERREUR] La construction de l'environnement a echoue.
    pause
    exit /b 1
)

echo.
echo [2/2] Execution du pipeline ETL...
echo       Cela peut prendre quelques minutes.
echo.
docker compose run --rm etl
set ETL_RESULT=%errorlevel%

echo.
if %ETL_RESULT% equ 0 (
    echo =====================================================
    echo   SUCCES : Donnees chargees dans le Datamart !
    echo   Les logs sont dans le dossier : logs\
    echo =====================================================
) else (
    echo =====================================================
    echo   ERREUR : Le pipeline a rencontre un probleme.
    echo   Consultez les logs dans le dossier : logs\
    echo =====================================================
)

echo.
pause
exit /b %ETL_RESULT%
