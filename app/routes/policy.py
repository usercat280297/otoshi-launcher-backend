"""
Policy Routes - Privacy Policy and Terms of Service API endpoints
"""

from fastapi import APIRouter
from pydantic import BaseModel
from typing import List, Optional

router = APIRouter()


class PolicySection(BaseModel):
    title: str
    content: List[str]
    warning: Optional[bool] = None


class PolicyContact(BaseModel):
    email: str
    message: str


class PrivacyPolicyData(BaseModel):
    lastUpdated: str
    introduction: str
    sections: List[PolicySection]
    contact: PolicyContact


class TermsOfServiceData(BaseModel):
    lastUpdated: str
    introduction: str
    sections: List[PolicySection]
    contact: PolicyContact


# Privacy Policy Data
PRIVACY_POLICY = PrivacyPolicyData(
    lastUpdated="2026-01-30",
    introduction="OTOSHI Launcher values your privacy. This policy explains what data we collect, how we use it, and the choices you have. By using OTOSHI Launcher, you agree to the practices described below.",
    sections=[
        PolicySection(
            title="Information we collect",
            content=[
                "Account data: email, username, display name, and login tokens.",
                "Device data: operating system, device identifiers, and app version.",
                "Usage data: library activity, download history, and feature usage.",
                "Community data: messages, reviews, and profile details you provide.",
            ],
        ),
        PolicySection(
            title="How we use your information",
            content=[
                "Provide and improve launcher services, downloads, and updates.",
                "Secure accounts and prevent fraud or abuse.",
                "Analyze performance and optimize user experience.",
                "Support community and social features you enable.",
            ],
        ),
        PolicySection(
            title="Sharing and disclosure",
            content=[
                "We do not sell your personal data.",
                "We may share limited data with service providers that help us operate the launcher (hosting, analytics, payment processing).",
                "We may also disclose information if required by law or to protect users and the platform.",
            ],
        ),
        PolicySection(
            title="Data retention",
            content=[
                "We retain data only as long as needed to provide services or comply with legal obligations.",
                "You may request deletion of your account and related data at any time.",
            ],
        ),
        PolicySection(
            title="Security",
            content=[
                "We use industry-standard safeguards to protect your data.",
                "No system is completely secure, but we continuously work to improve our protections.",
                "All data transmissions are encrypted using TLS 1.3.",
            ],
        ),
        PolicySection(
            title="Children",
            content=[
                "OTOSHI Launcher is not intended for children under the age of 13.",
                "If you believe a child has provided personal information, contact us to remove it.",
            ],
        ),
    ],
    contact=PolicyContact(
        email="support@otoshi-launcher.me",
        message="Questions about this policy? Contact us at",
    ),
)

# Terms of Service Data
TERMS_OF_SERVICE = TermsOfServiceData(
    lastUpdated="2026-01-30",
    introduction="Welcome to OTOSHI Launcher. These Terms of Service govern your use of our software and services. By accessing or using OTOSHI Launcher, you agree to be bound by these terms.",
    sections=[
        PolicySection(
            title="Acceptance of Terms",
            content=[
                "By downloading, installing, or using OTOSHI Launcher, you agree to these Terms of Service.",
                "If you do not agree to these terms, you must not use the software.",
                "We reserve the right to modify these terms at any time. Continued use after changes constitutes acceptance.",
            ],
        ),
        PolicySection(
            title="Account Registration",
            content=[
                "You must create an account to access certain features of OTOSHI Launcher.",
                "You are responsible for maintaining the confidentiality of your account credentials.",
                "You must provide accurate and complete information when creating your account.",
                "You are responsible for all activities that occur under your account.",
            ],
        ),
        PolicySection(
            title="Permitted Use",
            content=[
                "OTOSHI Launcher is provided for personal, non-commercial use only.",
                "You may download and install games through the launcher for your own use.",
                "You may use community features in accordance with our Community Guidelines.",
                "You may not reverse engineer, decompile, or modify the launcher software.",
            ],
        ),
        PolicySection(
            title="Prohibited Activities",
            content=[
                "Sharing, selling, or distributing your account credentials.",
                "Using automated tools, bots, or scripts to interact with the launcher.",
                "Attempting to bypass security measures or access restricted areas.",
                "Uploading malicious content or engaging in harmful activities.",
                "Violating intellectual property rights of third parties.",
            ],
            warning=True,
        ),
        PolicySection(
            title="Intellectual Property",
            content=[
                "OTOSHI Launcher and its original content are owned by OTOSHI and protected by copyright laws.",
                "Game content is owned by respective publishers and developers.",
                "Trademarks and logos are the property of their respective owners.",
                "You may not use our intellectual property without explicit permission.",
            ],
        ),
        PolicySection(
            title="Purchases and Payments",
            content=[
                "All purchases are final unless otherwise stated.",
                "Prices are subject to change without notice.",
                "We use secure third-party payment processors.",
                "Refunds are subject to our Refund Policy.",
            ],
        ),
        PolicySection(
            title="Termination",
            content=[
                "We may suspend or terminate your account for violations of these terms.",
                "You may delete your account at any time through account settings.",
                "Upon termination, your access to purchased content may be affected.",
                "Termination does not affect our right to pursue legal remedies.",
            ],
        ),
        PolicySection(
            title="Limitation of Liability",
            content=[
                "OTOSHI Launcher is provided 'as is' without warranties of any kind.",
                "We are not liable for any indirect, incidental, or consequential damages.",
                "Our total liability is limited to the amount you paid for our services.",
                "Some jurisdictions do not allow limitation of liability, so this may not apply to you.",
            ],
        ),
        PolicySection(
            title="Governing Law",
            content=[
                "These terms are governed by the laws of the applicable jurisdiction.",
                "Any disputes will be resolved through binding arbitration.",
                "You waive the right to participate in class action lawsuits.",
            ],
        ),
    ],
    contact=PolicyContact(
        email="legal@otoshi-launcher.me",
        message="For legal inquiries or questions about these terms, contact us at",
    ),
)


@router.get("/privacy", response_model=PrivacyPolicyData)
async def get_privacy_policy():
    """Get the privacy policy content"""
    return PRIVACY_POLICY


@router.get("/terms", response_model=TermsOfServiceData)
async def get_terms_of_service():
    """Get the terms of service content"""
    return TERMS_OF_SERVICE
